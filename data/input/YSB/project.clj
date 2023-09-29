;; Copyright 2015, Yahoo Inc.
;; Licensed under the terms of the Apache License 2.0. Please see LICENSE file in the project root for terms.

(defproject setup "0.1.0-SNAPSHOT"
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [org.clojure/tools.cli "1.0.219"]
                 [org.clojars.tavisrudd/redis-clojure "1.3.2"]
                 [clj-kafka "0.3.4"]
                 [clj-json "0.5.3"]
                 [clj-yaml "0.4.0"]]
  :main setup.core
  :jvm-opts ["-Xmx8g" "-server"]
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})

