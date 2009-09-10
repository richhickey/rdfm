;   Copyright (c) Rich Hickey. All rights reserved.
;   The use and distribution terms for this software are covered by the
;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;   which can be found in the file epl-v10.html at the root of this distribution.
;   By using this software in any fashion, you are agreeing to be bound by
;   the terms of this license.
;   You must not remove this notice, or any other, from this software.

(ns org.clojure.rdfm
  (:import 
   (java.util UUID)
   (javax.xml.datatype DatatypeFactory XMLGregorianCalendar)
   (org.openrdf.repository Repository RepositoryConnection)
   (org.openrdf.model Literal Resource Statement URI Value ValueFactory)))

(set! *warn-on-reflection* true)
(set! *print-meta* false)

;todo - make these private
(def #^"[Lorg.openrdf.model.Resource;" NOCONTEXT (make-array org.openrdf.model.Resource 0))
(def DATA-NS "http://clojure.org/data/")
(def SCALAR-NS (str DATA-NS "scalar/"))
(def KEYWORD-NS (str SCALAR-NS "keyword/"))
(def COLL-NS (str DATA-NS "collection/"))
(def VEC-NS (str COLL-NS "vector#"))
(def MAP-NS (str COLL-NS "hash-map#"))
(def #^String RDF-NS "http://www.w3.org/1999/02/22-rdf-syntax-ns#")
(def #^String RDF-NUMBER-NS "http://www.w3.org/1999/02/22-rdf-syntax-ns#_")
(def UUID-NS "urn:uuid:")
(def #^RepositoryConnection *conn*)
(def #^ValueFactory *vf*)

(defmacro dotrans [conn & body]
  `(let [conn# ~conn]
     (binding [*conn* conn#
               *vf* (.getValueFactory #^RepositoryConnection conn#)]
       (try
        (let [ret# (do ~@body)]
          (.commit *conn*)
          ret#)
        (catch Exception e#
          (.rollback *conn*)
          (throw e#))))))

(defn- kw->uri [k]
  (.createURI *vf* KEYWORD-NS (subs (str k) 1)))

(defn- uri->kw [uri]
  (keyword (subs (str uri) (count KEYWORD-NS))))

(defn random-uuid-uri [prefix]
  (java.net.URI. (str prefix (java.util.UUID/randomUUID))))

(declare store-1)

(defn- resource-for [id]
  (condp instance? id
    Resource     id
    java.net.URI (.createURI *vf* (str id))
    String       (.createURI *vf* id)))

(defn- value-for [o]
  (condp instance? o
    clojure.lang.IPersistentCollection (do (assert (::id ^o)) (resource-for (::id ^o)))
    Value o
    String (.createLiteral *vf* #^String o)
    Integer (.createLiteral *vf* (int o))
    Long (.createLiteral *vf* (long o))
    Float (.createLiteral *vf* (float o))
    Double (.createLiteral *vf* (double o))
    Boolean (.createLiteral *vf* (boolean o))
    java.net.URI  (.createURI *vf* (str o))
    clojure.lang.Keyword (kw->uri o)
    BigInteger (.createLiteral *vf* (str o) (.createURI *vf* "http://www.w3.org/2001/XMLSchema#integer"))
    BigDecimal (.createLiteral *vf* (str o) (.createURI *vf*  "http://www.w3.org/2001/XMLSchema#decimal"))
    XMLGregorianCalendar (.createLiteral *vf* #^XMLGregorianCalendar o)))


(def value-extractors
     {"" #(.getLabel #^Literal %)
      "http://www.w3.org/2001/XMLSchema#int" #(.intValue  #^Literal %)
      "http://www.w3.org/2001/XMLSchema#long" #(.longValue  #^Literal %)
      "http://www.w3.org/2001/XMLSchema#float" #(.floatValue  #^Literal %)
      "http://www.w3.org/2001/XMLSchema#double" #(.doubleValue  #^Literal %)
      "http://www.w3.org/2001/XMLSchema#boolean" #(.booleanValue  #^Literal %)
      "http://www.w3.org/2001/XMLSchema#integer" #(.integerValue  #^Literal %)
      "http://www.w3.org/2001/XMLSchema#decimal" #(.decimalValue  #^Literal %)
      
      "http://www.w3.org/2001/XMLSchema#dateTime" #(.calendarValue  #^Literal %)
      "http://www.w3.org/2001/XMLSchema#time" #(.calendarValue  #^Literal %)
      "http://www.w3.org/2001/XMLSchema#date" #(.calendarValue  #^Literal %)
      "http://www.w3.org/2001/XMLSchema#gYearMonth" #(.calendarValue  #^Literal %)
      "http://www.w3.org/2001/XMLSchema#gMonthDay" #(.calendarValue  #^Literal %)
      "http://www.w3.org/2001/XMLSchema#gYear" #(.calendarValue  #^Literal %)
      "http://www.w3.org/2001/XMLSchema#gMonth" #(.calendarValue  #^Literal %)
      "http://www.w3.org/2001/XMLSchema#gDay" #(.calendarValue  #^Literal %)})

(defn- extract-value [v]
  (condp instance? v
    URI (if (.startsWith (str v) KEYWORD-NS) 
          (uri->kw v) 
          v)
    Literal ((value-extractors (str (.getDatatype #^Literal v))) v)))
  

(defn- property-uri [k]
  (cond 
   (string? k) (.createURI *vf* k)
   (keyword? k) (kw->uri k)
   (integer? k) (.createURI  *vf* RDF-NS (str "_" (inc k)))
   :else (throw (IllegalArgumentException. (str "Unsupported key type: " (class k))))))

(defn- property-key [p]
  (let [id (str p)]
    (cond
     (.startsWith id KEYWORD-NS) (uri->kw p)
     (.startsWith id RDF-NUMBER-NS) (dec (-> id (subs 44) Integer/parseInt))
     :else id)))

(defn- add-statement [s p o]
  (.add *conn* (.createStatement *vf* s p o) NOCONTEXT))

(defmulti store-initial (fn [o & id] (class o)))

(defn store-meta [o]
  (let [ret-meta (dissoc ^o ::id)]
    (if (pos? (count ret-meta))
      (let [m (store-initial ret-meta (random-uuid-uri UUID-NS))]
        (add-statement (resource-for (::id ^o)) (property-uri ::meta) (resource-for (::id ^m)))
        (with-meta o (assoc m ::id (::id ^o))))
      o)))


(defmethod store-initial :default [x & id] x)

(defmethod store-initial clojure.lang.IPersistentVector [vec]
  (let [id (random-uuid-uri VEC-NS)
        res (resource-for id)
        ret (reduce (fn [vret i]
                      (let [v (store-initial (vec i))]
                        (add-statement res (property-uri i) (value-for v))
                        (conj vret v)))
                    [] (range (count vec)))
        ;ret (with-meta ret (assoc (meta vec) ::id id))
        ;ret (store-meta ret)
        ret (with-meta ret {::id id})]
    ret))

(defmethod store-initial clojure.lang.IPersistentMap
  ([m] (store-initial m (random-uuid-uri MAP-NS)))
  ([m id]
     (let [res (resource-for id)
           ret (reduce 
                (fn [mret [k v]]
                  (letfn [(store1 [v] 
                            (let [nested (store-initial v)]
                              (add-statement res (property-uri k) (value-for nested))
                              nested))]
                    (cond
                     (set? v) (assoc mret k (reduce (fn [s v] (conj s (store1 v))) #{} v))
                     :else (assoc mret k (store1 v)))))
                {} m)
           ret (with-meta ret (assoc (meta m) ::id id))
           ret (store-meta ret)]
       ret)))

(defn store-root 
  ([m] (store-root m (random-uuid-uri UUID-NS)))
  ([m uri] (store-initial m uri)))
  
(defn get-statements [id]
  (let [res (.getStatements *conn* (resource-for id) nil nil false NOCONTEXT)
        ret (into [] (.asList res))]
    (.close res)
    ret))

(defn remove-all [id]
  (.remove *conn* (resource-for id) nil nil NOCONTEXT))

(declare pull)

(defn restore-value [value]
  (let [v (extract-value value)]
    (if (instance? URI v)
      (let [id (str v)]
        (cond
         (.startsWith id COLL-NS) (pull id)
         :else (java.net.URI. (str v))))
      v)))

(defn setify [x]
  (if (set? x) x #{x}))

(defn uri->id [uri]
  (condp instance? uri
    String uri
    java.net.URI uri
    URI (java.net.URI. (str uri))))   
   
(defn pull [id]
  (let [statements (get-statements id)
        id-str (str id)]
    (cond
     (.startsWith id-str VEC-NS) (with-meta
                                       (reduce (fn [v #^Statement s]
                                                 (let [p (property-key (.getPredicate s))
                                                       o (.getObject s)]
                                                   (assoc v p (restore-value o))))
                                               (vec (repeat (count statements) nil))
                                               statements)
                                       {::id id-str})
     :else (vary-meta
            (reduce (fn [m #^Statement s]
                      (let [k (property-key (.getPredicate s))
                            v (restore-value (.getObject s))]
                        (cond 
                         (= k ::meta) (with-meta m v)
                         (contains? m k) (assoc m k (conj (setify (m k)) v))
                         :else (assoc m k v)))) {} statements)
            assoc ::id (uri->id id)))))
   
(defn assoc-1 [idcoll k v]
  (let [id (if (coll? idcoll) (::id (meta idcoll)) idcoll)
        p (property-uri k)
        val (value-for v)]
    ))
    
  
                      
(comment
;fiddle
(import
 (org.openrdf.repository Repository RepositoryConnection)
 (org.openrdf.repository.sail SailRepository)
 (org.openrdf.repository.http HTTPRepository)
 (org.openrdf.sail.memory MemoryStore))

(alias 'rdfm 'org.clojure.rdfm)
(def #^Repository repo (SailRepository. (MemoryStore. (java.io.File. "/Users/rich/dev/data/db"))))
(.initialize repo)
(def c (.getConnection repo))

(def x (rdfm/dotrans c
                (rdfm/store-root {:a 1 :b 2 :c [3 4 [5 6 {:seven :eight}]] :d "six" :e {:f 7 :g #{8}}})))
(def xs (rdfm/dotrans c (rdfm/pull (::org.clojure.rdfm/id ^x))))

(.close c)
(.shutDown repo)

;cruft needed for dates
;import java.util.GregorianCalendar;
;import javax.xml.datatype.DatatypeFactory;
;import javax.xml.datatype.XMLGregorianCalendar
;GregorianCalendar c = new GregorianCalendar();
;c.setTime(yourDate);
;XMLGregorianCalendar date2 = DatatypeFactory.newInstance().newXMLGregorianCalendar(c);
;(def #^DatatypeFactory dtf (DatatypeFactory/newInstance))

)
    
  

