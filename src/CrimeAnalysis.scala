import breeze.numerics.{abs, pow, sqrt}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
 * Created by Conley on 2016/5/6.
 */


object CrimeAnalysis{

  def main(args: Array[String]) {
    /**
     * 换种思路，先生成全部的顶点的RDD，然后使用RDD的cartesian函数获取笛卡尔积，构造边
     * distance获得的距离是一个元组，包含各种距离，可以拓展
     * 1.传播用的顶点需要简化为只包含id和标记向量
     * 2.传播用的图简化为只包含两个顶点id和边的权重
     * 注意：无法输出为单个的文本文件需要后期处理
     */

    val trainFilePath: String = "hdfs://cat:9000/data/crime/train0.csv"
    val testFilePath: String =  "hdfs://cat:9000/data/crime/test0.csv"
    val testOutPutFilePath:String =  "hdfs://cat:9000/data/crime/results"

    val conf = new SparkConf()
      .setAppName("crimeAnalysis1.3.beta")
      .setMaster("spark://cat:7077")
      .set("spark.executor.memory","5g")
//      .setJars(List("F:\\Spark\\Lab\\CrimeAnalysis1.3\\out\\artifacts\\CrimeAnalysis1_3_jar\\CrimeAnalysis1.3.jar"))
//      .set("spark.executor.extraJavaOptions", "-Xdebug -Xrunjdwp:transport=dt_socket,address=8002,server=y,suspend=n")


    val sc = new SparkContext(conf);

    val parallelismNum = sc.defaultParallelism*2
    val leastSim = 0.1

    //////辅助函数模块1：用于确定‘字符标签’所对应的数字标签==========================================================================
    val rawTag = "ARSON,ASSAULT,BAD CHECKS,BRIBERY,BURGLARY,DISORDERLY CONDUCT,DRIVING UNDER THE INFLUENCE,DRUG/NARCOTIC,DRUNKENNESS,EMBEZZLEMENT,EXTORTION,FAMILY OFFENSES,FORGERY/COUNTERFEITING,FRAUD,GAMBLING,KIDNAPPING,LARCENY/THEFT,LIQUOR LAWS,LOITERING,MISSING PERSON,NON-CRIMINAL,OTHER OFFENSES,PORNOGRAPHY/OBSCENE MAT,PROSTITUTION,RECOVERED VEHICLE,ROBBERY,RUNAWAY,SECONDARY CODES,SEX OFFENSES FORCIBLE,SEX OFFENSES NON FORCIBLE,STOLEN PROPERTY,SUICIDE,SUSPICIOUS OCC,TREA,TRESPASS,VANDALISM,VEHICLE THEFT,WARRANTS,WEAPON LAWS";
    val tagArray = rawTag.split(",")
    val maxTagLen = tagArray.length;
    def getTagIndex(e: String): Int = {
      if (tagArray.contains(e)) tagArray.indexOf(e) else -1
    }
    def getTagAt(n: Int): String = {
      if (n > 0 && n < maxTagLen) tagArray(n) else null
    }
    ////========================================================================================================================
    ////辅助函数模块2：用于生成全局唯一id标识==
    // 不起作用，每个partition会自己独立统计，不能做到统一id，抛弃
    ////辅助函数模块3：顶点距离或者相似度计算=======================================================================================

    val timetype = """(\d\d\d\d)-(\d\d)-(\d\d)\s{1}(\d\d):(\d\d):(\d\d)""".r

    def tdst(a: Double, b: Double, max: Int): Double = {
      val temp: Int = max / 2;
      val intval: Double = a - b;
      val result = if (abs(intval) > temp) (temp - intval) else intval
      result
    }
    def week2num(a: String): Int = {
      a match {
        case "Monday" => 1
        case "Tuesday" => 2
        case "Wednesday" => 3
        case "Thursday" => 4
        case "Friday" => 5
        case "Saturday" => 6
        case "Sunday" => 7
      }
    }

    def platcosim(src: (String, String, Double, Double, (Boolean, Array[Double])),
                  dst: (String, String, Double, Double, (Boolean, Array[Double]))
                   ): Double = {
      //影响因子：
      val klog: Double = 0.45 //经度占比
      val klat: Double = 0.45 //维度占比
      val kwk: Double = 0.55 //星期占比
      val kmo: Double = 0.32 //月份占比
      val kdy: Double = 0.22 //日期占比
      val kh: Double = 0.32 //小时占比（根号0.1）
      val kmi: Double = 0.22 //分钟占比

      val srctime = src._1;
      //格式为：“yyyy-mm-dd hh:mm:ss”
      val dsttime = dst._1;
      val (dmonth1, dday1, dhour1, dminutes1) = srctime match {
        case timetype(srcyear, srcmonth, srcday, srchour, srcminutes, srcseconds) => {
          //计算去中心化
          val dmonth1 = kmo * srcmonth.toDouble
          val dday1 = kdy * srcday.toDouble
          val dhour1 = kh * srchour.toDouble
          val dminutes1 = kmi * srcminutes.toDouble
          (dmonth1, dday1, dhour1, dminutes1)
        }
      }

      val (dmonth2, dday2, dhour2, dminutes2) = srctime match {
        case timetype(dstyear, dstmonth, dstday, dsthour, dstminutes, dstseconds) => {
          val dmonth2 = kmo * dstmonth.toDouble
          val dday2 = kdy * dstday.toDouble
          val dhour2 = kh * dsthour.toDouble
          val dminutes2 = kmi * dstminutes.toDouble
          (dmonth2, dday2, dhour2, dminutes2)
        }
      }

      val srcweeknum = week2num(src._2);
      val dstweeknum = week2num(dst._2);
      //result 3
      val dweek1 = kwk * srcweeknum.toDouble
      val dweek2 = kwk * dstweeknum.toDouble
      //result 4 about latitude and logitude
      val dlog1 = klog * src._3.toDouble
      val dlog2 = klog * dst._3.toDouble
      val dlat1 = klat * src._4.toDouble
      val dlat2 = klat * dst._4.toDouble

      //利用上面的变量可以组成两个向量：（dmonth1,dday1,dhour1,dminutes1,dweek1,dlog1,dlat1）,（dmonth2,dday2,dhour2,dminutes2,dweek2,dlog2,dlat2）
      (dmonth1 * dmonth2 + dday1 * dday2 + dhour1 * dhour2 + dminutes1 * dminutes2 + dweek1 * dweek2 + dlog1 * dlog2 + dlat1 * dlat2) /
        (sqrt(
          (dmonth1 * dmonth1 + dday1 * dday1 + dhour1 * dhour1 + dminutes1 * dminutes1 + dweek1 * dweek1 + dlog1 * dlog1 + dlat1 * dlat1)
            * (dmonth2 * dmonth2 + dday2 * dday2 + dhour2 * dhour2 + dminutes2 * dminutes2 + dweek2 * dweek2 + dlog2 * dlog2 + dlat2 * dlat2)
        )
          )
    }

    ////==========================================================================================================================

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    //生成最初版本的顶点元素
    //最原始数据格式：Tag,Longitude，Latitude，Month，Day，Week，Hour，Minutes
    //初步解析的数据（rawTrainData）格式:(timeStamp(String), category(String), description(Sring), week(String),PdDistrict(String),Resolution(String),Address(String),X(String),Y(String))
    //                                       0                   1                      2          3                 4                   5                 6         7         8
    //  顶点和数据元素:( timeStamp(String), week(String), longgitude(Double), latitude(Double), (canChange,probs(Array[double])), id )
    //                         1                2                 3               4                         5
    //

    val splitReg = ",(?=(?:[^\\\"]*\\\"[^\\\"]*\\\"[^\\\"]*)*$|[^\"]*$)"



    //根据训练数据集制作顶点集：TrainData => laveledVertexSet

    val labeledVertexRDD = sc.textFile(trainFilePath).mapPartitionsWithIndex((idx,lines)=>{
      if (idx == 0) {//去掉首行
        lines.drop(1)
      }
      println("=========== a labeled vertex added ==============")
      lines.map(s => s+","+idx.toString)
    })
      .map(_.split(splitReg))
      .map(attr => {
      //根据attr(1)(category)做出一个概率向量
      val probs = new Array[Double](maxTagLen)
      //      probs(i) = 1.0
      (attr(0), attr(3), attr(7).toDouble, attr(8).toDouble, (false, probs))
    }).keyBy(attr => attr.hashCode().toLong)

    //存储临时结果：labeledVertexRDD
    //    labeledVertexRDD.saveAsTextFile(labeledVertexFilePath)
    ////================================================================================================================================

    /////////////////////////////////构建查询顶点，其中的标签值为空，填入属性值，且与已有图进行Join
    //数据格式：Id,Dates,DayOfWeek,PdDistrict,Address,X,Y
    //  顶点和数据元素:( timeStamp(String), week(String), longgitude(Double), latitude(Double), (canChange,probs(Array[double])),id(Long) )
    //                         1                2                 3               4                         5
    val unlabeledVertexRDD = sc.textFile(testFilePath).mapPartitionsWithIndex((idx,lines)=>{
      if (idx == 0) {//去掉首行
        lines.drop(1)
      }
      println("============= a unlabeled vertex added ==================")
      lines
    })
      .map(_.split(splitReg))
      .map(attr => {
      val probs = new Array[Double](maxTagLen)
      (attr(0).toLong,(attr(1), attr(2), attr(5).toDouble, attr(6).toDouble, (true, probs)))
    })

    //    unlabeledVertexRDD.saveAsTextFile(unlabeledVertexFilePath)

    //利用这些顶点数据生成VertexRDD和EdgeRDD,只计算单向传播，已标注到未标注
    val edgeSet: RDD[Edge[Double]] =
      labeledVertexRDD
        .cartesian(unlabeledVertexRDD)
        .map(v => {
        val resultEdge = {
          //构建了全连通图，相似度小于0的被置为0
          val sim = platcosim(v._1._2, v._2._2)
          if(sim > leastSim){
            println("================== a new edge created ==========================")
            new Edge(v._1._1, v._2._1, sim)
          }else null
        }
        resultEdge
      }).filter(e => e!=null).cache()

    val platLabeledVertex = labeledVertexRDD.map(v => (v._1,v._2._5))
    val platUnlabeledVertex = unlabeledVertexRDD.map(v => (v._1,v._2._5))
    labeledVertexRDD.unpersist()
    unlabeledVertexRDD.unpersist()

    val platVertexSet = (platLabeledVertex ++ platUnlabeledVertex).cache()
    val graph = Graph(platVertexSet, edgeSet).cache()

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////进行标签传播算法////////////////////////////////////////////////////////////////////////////////////////
    //---------------------------------图构建完毕-----------------------------------
    //--图顶点信息为（id[Long],(isBaseLabel[Boolean],flagArray[Double])）
    //--边信息为：（id[Long],id[Long],weight[double]）
    // 下面进行标签传播-----------------------------------

    //自定义的标签传播算法，默认的不太好，需要我们自己来实现
//    val alpha = 0.3;
//    val maxSteps = 20; //最大迭代次数
//
//    val lpaGraph = graph;
//
//    //On the first iteration all vertices receive the initialMsg
//    // and on subsequent iterations if a vertex does not receive a message then the vertex-program is not invoked.
//    val initialMessage = new Array[Double](maxTagLen)
//
//    //vprog: (VertexId, VD, A) ⇒ VD
//    //The user-defined vertex-program vprog is executed in parallel
//    // on each vertex receiving any inbound messages and computing a new value for the vertex.
//    def vertexProgram(vid: VertexId, attr: (Boolean, Array[Double]), message: Array[Double] )
//    : (Boolean, Array[Double]) = {
//      val resultAttr = if ((attr._1) && (message != null)&&(message.length>0)) {
//        //can change
//
//        val selfprob = attr._2;
//        val neoprob = selfprob.zip(message).map(z => z._1+z._2)
//        val totalscore = neoprob.sum;
//        val finalprob = neoprob.map(a => if(totalscore!=0)(a/totalscore)else a)
//        val result = (attr._1,finalprob)
//        result
//      } else attr
//
//      resultAttr
//    }
//
//    // sendMsg: (EdgeTriplet[VD, ED]) ⇒ Iterator[(VertexId, A)]
//    //The sendMsg function is then invoked on all out-edges and is used to compute an optional message to the destination vertex.
//    def sendMessage(e: EdgeTriplet[(Boolean, Array[Double]), Double])
//    : Iterator[(VertexId, Array[Double])] = {
//
//      val selfprob = e.srcAttr._2
//      val sim: Double = e.attr
//      val neomsg = selfprob.map(p => p*sim)
//      val msg = if (e.srcId < e.dstId) {
//        Iterator((e.dstId, neomsg))
//      } else {
//        Iterator((e.srcId, neomsg))
//      }
//      msg;
//    }
//
//    //mergeMsg: (A, A) ⇒ A)
//    //The mergeMsg function is a commutative associative function used to combine messages destined to the same vertex.
//    def mergeMessage(msg1: Array[Double], msg2: Array[Double])
//    : Array[Double] = {
//      val probs = msg1.zip(msg2).map(m => m._1+m._2)
//      probs
//    }
//
//
//    //    //调用Graphx的核心库Pregel进行迭代操作
//    val labeledGraph = Pregel(lpaGraph, initialMessage, maxIterations = maxSteps)(
//      vprog = vertexProgram,
//      sendMsg = sendMessage,
//      mergeMsg = mergeMessage)
//
//    val mypredict = labeledGraph.vertices.filter(v => v._2._1==true).sortByKey().repartition(1)
//
//    //    var pre:String = "Id,ARSON,ASSAULT,BAD CHECKS,BRIBERY,BURGLARY,DISORDERLY CONDUCT,DRIVING UNDER THE INFLUENCE,DRUG/NARCOTIC,DRUNKENNESS,EMBEZZLEMENT,EXTORTION,FAMILY OFFENSES,FORGERY/COUNTERFEITING,FRAUD,GAMBLING,KIDNAPPING,LARCENY/THEFT,LIQUOR LAWS,LOITERING,MISSING PERSON,NON-CRIMINAL,OTHER OFFENSES,PORNOGRAPHY/OBSCENE MAT,PROSTITUTION,RECOVERED VEHICLE,ROBBERY,RUNAWAY,SECONDARY CODES,SEX OFFENSES FORCIBLE,SEX OFFENSES NON FORCIBLE,STOLEN PROPERTY,SUICIDE,SUSPICIOUS OCC,TREA,TRESPASS,VANDALISM,VEHICLE THEFT,WARRANTS,WEAPON LAWS\n"
//    val tempResult = mypredict.map(v => {
//      val id = v._1
//      var pre:String = id.toString
//      v._2._2.foreach(a => pre= pre +","+a )
//      pre
//    })
//    tempResult.saveAsTextFile(testOutPutFilePath)

  }

}
