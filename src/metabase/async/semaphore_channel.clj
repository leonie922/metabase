(ns metabase.async.semaphore-channel
  (:require [clojure.core.async :as a]
            [clojure.tools.logging :as log]
            [metabase.async.util :as async.u]
            [metabase.util :as u]
            [metabase.util.i18n :refer [trs]])
  (:import java.io.Closeable
           java.lang.ref.WeakReference))

(defn- permit [id return!]
  (let [closed? (atom false)]
    (reify
      Object
      (toString [this]
        (format "Permit #%d" id))

      Closeable
      (close [this]
        (when-not @closed?
          (when (compare-and-set! closed? false true)
            (return! id)))
        nil))))

(defn- new-permit! [{:keys [id-counter closed-permits-in-chan permits weak-refs]}]
  (let [id       (swap! id-counter inc)
        permit   (permit id #(a/>!! closed-permits-in-chan %))
        weak-ref (WeakReference. permit)]
    (log/debug (trs "Created {0}" permit))
    (swap! weak-refs assoc id weak-ref)
    (a/>!! permits permit)
    nil))

(defn- handle-closed-permits
  "loop to handle returned permits"
  [{:keys [closed-permits-in-chan weak-refs], :as context}]
  (a/go-loop []
      (let [id    (a/<! closed-permits-in-chan)
            [old] (swap-vals! weak-refs dissoc id)]
        (when (get old id)
          (log/debug (trs "Permit {0} returned nicely" id))
          (new-permit! context)))
      (recur)))

(defn- cleanup-orphaned-permits [{:keys [weak-refs], :as context}]
  (log/debug (trs "Initiating clean up of orphaned permits. Total weak refs: {0}" (count @weak-refs)))
  (doseq [[id, ^WeakReference weak-ref] @weak-refs]
    (log/debug (trs "Check permit {0} orphaned? {1}" id (nil? (.get weak-ref))))
    (when-not (.get weak-ref)
      (log/warn (trs "Warning: orphaned Permit {0} recovered. Make sure to close permits when done!" id))
      (let [[old] (swap-vals! weak-refs dissoc id)]
        (when (get old id)
          (new-permit! context))))))

(defn- furnish-available-permits
  "loop to take returned permits and make them available to the `out` channel consumed elsewhere, or to clean up"
  [{:keys [available-permits-out-chan permits weak-refs], :as context}]
  (a/go-loop [permit (a/poll! permits)]
    (if permit
      (do
        (a/>! available-permits-out-chan permit)
        (recur (a/poll! permits)))
      ;; Out of permits. Cleanup time!
      (do
        (cleanup-orphaned-permits context)
        (recur (a/<! permits))))))


(defn semaphore-channel
  "Create a new channel that acts as a counting semaphore, containing `n` permits. Use `<!` or the like to get a permit
  from the semaphore channel. The permit should be returned by calling `.close` or by using `with-open`."
  [n]
  (let [context {:id-counter                 (atom 0)
                 :weak-refs                  (atom {})
                 :permits                    (a/chan n)
                 :closed-permits-in-chan     (a/chan n)
                 :available-permits-out-chan (a/chan 1)}]
    ;; load the semaphore channel with the initial n permits
    (dotimes [_ n]
      (new-permit! context))
    ;; start loops that will take in closed permits, and another that will send them back out, scavenging for orphaned
    ;; permits if none are available
    (handle-closed-permits context)
    (furnish-available-permits context)
    (:available-permits-out-chan context)))


;;; ------------------------------------------- do-after-receiving-permit --------------------------------------------

(def ^:private ^:dynamic *permits* nil)

(defn- do-f-with-permit
  "Once a `permit` is obtained, execute `(apply f args)`, writing the results to `output-chan`, and returning the permit
  no matter what."
  [permit out-chan f & args]
  (log/debug (trs "Obtained {0}" permit))
  (try
    (let [f (fn []
              (with-open [permit permit]
                (try
                  (u/prog1 (apply f args)
                    (log/debug (trs "(f) finished, permit will be returned")))
                  (catch Throwable e
                    (log/error e "Caught unexpected exception")
                    e))))]
      (a/go
        (when (a/<! (async.u/single-value-pipe (async.u/do-on-separate-thread f) out-chan))
          (log/debug "request canceled, permit will be returned")
          (.close permit))))
    (catch Throwable e
      (log/error e (trs "Unexpected error attempting to run function after obtaining permit"))
      (a/>! out-chan e)
      (.close permit))))

(defn- do-after-waiting-for-new-permit [semaphore-chan f & args]
  (let [out-chan (a/chan 1)]
    ;; fire off a go block to wait for a permit.
    (a/go
      (let [[permit first-done] (a/alts! [semaphore-chan out-chan])]
        (binding [*permits* (assoc *permits* semaphore-chan permit)]
          ;; If out-chan closes before we get a permit, there's nothing for us to do here. Otherwise if we got our
          ;; permit then proceed
          (if (= first-done out-chan)
            (log/debug (trs "Not running pending function call: output channel already closed."))
            ;; otherwise if channel is still open run the function
            (apply do-f-with-permit permit out-chan f args)))))
    ;; return `out-chan` which can be used to wait for results
    out-chan))

(defn do-after-receiving-permit
  "Run `(apply f args)` asynchronously after receiving a permit from `semaphore-chan`. Returns a channel from which you
  can fetch the results. Closing this channel before results are produced will cancel the function call."
  {:style/indent 1}
  [semaphore-chan f & args]
  ;; check and see whether we already have a permit for `semaphore-chan`, if so, go ahead and run the function right
  ;; away instead of waiting for *another* permit
  (if (get *permits* semaphore-chan)
    (do
      (log/debug (trs "Current thread already has a permit for {0}, will not wait to acquire another" semaphore-chan))
      (async.u/do-on-separate-thread f))
    ;; otherwise wait for a permit
    (apply do-after-waiting-for-new-permit semaphore-chan f args)))
