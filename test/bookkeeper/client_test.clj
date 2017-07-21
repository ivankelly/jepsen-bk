(ns bookkeeper.client-test
  (:require [clojure.test :refer :all]
            [bookkeeper.client :refer :all])
  (:import [org.apache.bookkeeper.client
            BookKeeperTest BookKeeper$DigestType]))

(defn bk-fixture
  [f]
  (let [fixture (BookKeeperTest. BookKeeper$DigestType/MAC)]
    (.setUp fixture)
    (f)
    (.tearDown fixture)))

(deftest a-test
  (testing "FIXME, I fail."
    (is (= 0 1))))
