import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by xiaozhou on 5/15/17.
  */

case class WikipediaArticle(title: String, text: String) {
  /**
    * @return Whether the text of this article mentions `lang` or not
    * @param lang Language to look for (e.g. "Scala")
    */
  def mentionsLanguage(lang: String): Boolean = text.split(' ').contains(lang)
}

object ProLangRanking {

  def occurrencesOfLang(lang: String, rdd: RDD[WikipediaArticle]): Int = {
    val containsLangRdd = rdd.filter((x: WikipediaArticle) => x.mentionsLanguage(lang))
    containsLangRdd.count().toInt
  }

  def rankLangs(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] = {
    langs.map((lang:String) => (lang, occurrencesOfLang(lang, rdd))).sortBy(_._2)(Ordering.Int.reverse)
  }

  def makeIndex(langs: List[String], rdd: RDD[WikipediaArticle]): RDD[(String, Iterable[WikipediaArticle])] = {
    val langWikiarticle = rdd.flatMap((article: WikipediaArticle) => {
      var resSeq = Seq[(String, WikipediaArticle)]()
      val langsInArticle = langs.filter((lang: String) => article.mentionsLanguage(lang))
      for(lang <- langsInArticle){
        resSeq = resSeq :+ (lang, article)
      }
      resSeq
    })
    langWikiarticle.groupByKey()
  }

  def rankLangsUsingIndex(index: RDD[(String, Iterable[WikipediaArticle])]): List[(String, Int)] = {
    index.countByKey().toList.map((x: (String, Long)) => (x._1, x._2.toInt)).sortBy(_._2)(Ordering.Int.reverse)
  }

  def rankLangsReduceByKey(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] = {
    val langWikiarticle = rdd.flatMap((article: WikipediaArticle) => {
      var resSeq = Seq[(String, Int)]()
      val langsInArticle = langs.filter((lang: String) => article.mentionsLanguage(lang))
      for(lang <- langsInArticle){
        resSeq = resSeq :+ (lang, 1)
      }
      resSeq
    })
//      val result = langWikiarticle.reduceByKey(_+_).collect().sortBy(_._2)(Ordering.Int.reverse)
    langWikiarticle.countByKey().toList
      .map((x: (String, Long)) => (x._1, x._2.toInt)).sortBy(_._2)(Ordering.Int.reverse)
  }
  def main(args: Array[String]): Unit =  {

    val langs = List(
      "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
      "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")

    val conf: SparkConf = new SparkConf().setAppName("ProLangRanking")
    val sc: SparkContext = new SparkContext(conf)

    val wikiRdd: RDD[WikipediaArticle] = sc.textFile("/user/xiaozhou/CSS_A1/wikipedia/wikipedia.dat").map((x:String) => parse(x))


    /* Languages ranked according to (1) */
    val langsRanked: List[(String, Int)] = timed("Part 1: naive ranking", rankLangs(langs, wikiRdd))

    /* An inverted index mapping languages to wikipedia pages on which they appear */
    def index: RDD[(String, Iterable[WikipediaArticle])] = makeIndex(langs, wikiRdd)

    /* Languages ranked according to (2), using the inverted index */
    val langsRanked2: List[(String, Int)] = timed("Part 2: ranking using inverted index", rankLangsUsingIndex(index))

    /* Languages ranked according to (3) */
    val langsRanked3: List[(String, Int)] = timed("Part 3: ranking using reduceByKey", rankLangsReduceByKey(langs, wikiRdd))

    /* Output the speed of each ranking */
    println(timing)
    sc.stop()
  }

  val timing = new StringBuffer
  def timed[T](label: String, code: => T): T = {
    val start = System.currentTimeMillis()
    val result = code
    val stop = System.currentTimeMillis()
    timing.append(s"Processing $label took ${stop - start} ms.\n")
    result
  }

  def parse(line: String): WikipediaArticle = {
    val subs = "</title><text>"
    val i = line.indexOf(subs)
    val title = line.substring(14, i)
    val text  = line.substring(i + subs.length, line.length-16)
    WikipediaArticle(title, text)
  }
}