(ns bookkeeper.client
  "Bookkeeper client for clojure"
  (:require [manifold.deferred :as d])
  (:import [org.apache.bookkeeper.client LedgerHandle LedgerEntry BookKeeper
            BookKeeper$DigestType BKException BKException$Code
            AsyncCallback$CreateCallback AsyncCallback$OpenCallback
            AsyncCallback$CloseCallback AsyncCallback$AddCallback
            AsyncCallback$ReadCallback]
           [org.apache.bookkeeper.conf ClientConfiguration]))

(def default-bookkeeper-opts {:zookeeper/connect "localhost"})

(def default-ledger-opts {:ensemble-size 3
                          :quorum-size 2
                          :digest-type BookKeeper$DigestType/MAC
                          :password (.getBytes "bk-jepsen")})

(defn ^org.apache.bookkeeper.client.BookKeeper bookkeeper
  "Initialize a bookkeeper client"
  [opts]
  (let [opts (merge default-bookkeeper-opts opts)]
    (let [conf (doto (ClientConfiguration.)
                 (.setZkServers (:zookeeper/connect opts)))]
      (BookKeeper. conf))))

(defn close
  "Closes a bookkeeper client"
  [client]
  (.close client))

(defn- exception-or-result
  [deferred rc result]
  (if (= rc BKException$Code/OK)
    (d/success! deferred result)
    (d/error! deferred (BKException/create rc))))

(defn create-ledger
  "Create a new ledger"
  ([^BookKeeper client]
   (create-ledger client {}))
  ([^BookKeeper client opts]
   (let [opts (merge default-ledger-opts opts)
         deferred (d/deferred)
         cb (reify AsyncCallback$CreateCallback
              (createComplete [this rc ledger-handle ctx]
                (exception-or-result deferred rc ledger-handle)))]
     (.asyncCreateLedger client
                         (:ensemble-size opts)
                         (:quorum-size opts)
                         (:digest-type opts)
                         (:password opts)
                         cb nil)
     deferred)))

(defn- open-cb [deferred]
  (reify AsyncCallback$OpenCallback
    (openComplete [this rc ledger-handle ctx]
      (exception-or-result deferred rc ledger-handle))))

(defn open-ledger
  "Opens a ledger for reading"
  ([^BookKeeper client ledger-id]
   (open-ledger client ledger-id {}))
  ([^BookKeeper client ledger-id opts]
   (let [opts (merge default-ledger-opts opts)
         deferred (d/deferred)
         cb (open-cb deferred)]
     (.asyncOpenLedger client ledger-id
                       (:digest-type opts)
                       (:password opts)
                       cb nil)
     deferred)))

(defn open-ledger-no-recovery
  "Opens a ledger without recovery. You can only read to last add confirmed"
  ([^BookKeeper client ledger-id]
   (open-ledger-no-recovery client ledger-id {}))
  ([^BookKeeper client ledger-id opts]
   (let [opts (merge default-ledger-opts opts)
         deferred (d/deferred)
         cb (open-cb deferred)]
     (.asyncOpenLedgerNoRecovery client ledger-id
                                 (:digest-type opts)
                                 (:password opts)
                                 cb nil)
     deferred)))

(defn close-ledger [^LedgerHandle ledger-handle]
  "Close a ledger"
  (let [deferred (d/deferred)
        cb (reify AsyncCallback$CloseCallback
             (closeComplete [this rc ledger-handle ctx]
               (exception-or-result deferred rc nil)))]
    (.asyncClose ledger-handle cb nil)
    deferred))

(defn add-entry [^LedgerHandle ledger-handle bytes]
  "Asynchronously add an entry to a ledger, returns a promise"
  (let [deferred (d/deferred)
        cb (reify AsyncCallback$AddCallback
             (addComplete [this rc ledger-handle entryId ctx]
               (exception-or-result deferred rc entryId)))]
    (.asyncAddEntry ledger-handle bytes cb nil)
    deferred))

(defn read-entries [^LedgerHandle ledger-handle first-entry last-entry]
  "Read entries from first-entry to last-entry from a ledger."
  (let [deferred (d/deferred)
        return (d/chain deferred
                        (fn [entries]
                          (map (fn [e]
                                 [(.getEntryId e) (.getEntry e)])
                               (enumeration-seq entries))))
        cb (reify AsyncCallback$ReadCallback
             (readComplete [this rc ledger-handle entries ctx]
               (exception-or-result deferred rc entries)))]
    (.asyncReadEntries ledger-handle first-entry last-entry cb nil)
    return))

(defn ledger-id [^LedgerHandle ledger-handle]
  "Get the id of a ledger, from its handle"
  (.getId ledger-handle))

(defn last-add-confirmed [^LedgerHandle ledger-handle]
  "Get last add confirmed of a ledger"
  (.getLastAddConfirmed ledger-handle))
