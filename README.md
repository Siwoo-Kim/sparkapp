시작하기전..
==
해당 문서는 스파크 개념 & 용어 간략 설명.

스파크 Spark   @spark
-
클러스터 환경에서 데이터 빅데이터 를 병렬로 처리하는 computing engine<br/>
데이터 연산 역할만 수행 -> 영구 저장소 역할은 수행하지 않음.<br/>
cluster=여러 컴퓨터의 자원을 모아놓은 집합

   
스파크의 필요성
-
problem=단일 프로세스의 성능 향상 한계.
solution=성능 향상을 위해 병렬 cpu 코어를 추가하는 방식을 채택. (병렬 처리의 필요성)
    
영구저장소 Storage   @storage
-
* Azure Storage
* Amazon S3
* Apache Hadoop

하둡  @hadoop
-
클러스터 환경에서 컴퓨팅 시스템 (MapReduce) 와 하둡 파일시스템 (hdfs) 을 지원하는 플랫폼. <br/>
둘 중 하나의 시스템만 단독으로 사용하기 어려움.

클러스터    @cluster
-
다수의 컴퓨터의 자원을 모아 놓은 집합체.<br/>

클러스터 매니저의 종류.   @cluster manager
-
사용자의 앱 제출 submit 을 처리, 클러스터 리소스 관리.
* Standalone
* 하둡 YARN

스파크 어플리케이션. @application
-
하나의 driver 프로세스와 복수의 executors 프로세스로 구성.

Driver 프로세스. @driver
-
* 클러스터상 하나의 노드에서 실행하며 main 함수로 실행.
* 스파크 정보 (spark configuration) 정보 유지.
* executors 프로세스에 작업 분석, 할당, 스케줄링 역할을 담당.
* 스파크 application 의 갯수 = driver 프로세스 갯수.
* SparkSession = driver 프로세스의 접근 interface

<pre>
val spark = SparkSession.builder()  //드라이버 프로세스 시작.
        .appName("sparkapp")
        .master("local")
        .getOrCreate()

spark.range(100).toDF("num").where("num % 2 == 0").show()   
    //드라이버 프로세스에게 작업 요청. 실행 계획(DAG) 작성 후 액션 실행. 
</pre>

익스큐터 프로세스. @executor
-
Driver 프로세스가 할당한 작업을 수행. 익스큐터의 갯수는 병렬성과 연관있다.

분산 컬렉션  @distributed collection
-   
* 추상적으론 DataFrame, Dataset, RDD 을 의미.
* 물리적으론 입력 소스로 부터 받아 여러 노드에 분산된 데이터를 의미.
* 이때 분산된 하나의 데이터 집합을 Partition 이라 부름.
  
파티션 @partition
-
다수의 Executor 프로세스가 병렬로 작업할 수 있도록 분할된 청크 단위의 데이터. <br/>
파티션이 하나라면 복수의 Executor 라도 병렬성은 1. <br/> 
그 반대도 1 (하나의 Executor 복수의 파티션.)

    spark.conf.set("spark.sql.shuffle.partitions", 5)
    //shuffle 시 생성되는 파티션의 갯수 설정
넓은 의존성  @wide dependency
-
* Transformation 의 한 종류. 
* 하나의 입력 파티션이 둘 이상 파티션에 영향을 끼치는 연산.
* 다른 파티션의 데이터의 변경이 요구되므로 노드간 파티션 교환 작업 (shuffle) 이 요구됨.
* Transformation=분산 컬렉션을 변경하는 연산.

스파크 설치 & 실행 @install
-
1.  스파크 다운로드 http://spark.apache.org/downloads.html
2.  스파크 빌드.
    <pre>
    siwoo@siwoo-ubuntu:~/Downloads$ tar -xvf spark-2.4.5-bin-hadoop2.7.tgz
    siwoo@siwoo-ubuntu:~/Downloads$ sudo mv spark-2.4.5-bin-hadoop2.7 /usr/local/spark-2.4.5
    </pre>
3. 스파크 쉘 실행.
    <pre>
    siwoo@siwoo-ubuntu:~/work/workspace/sparkapp$ SPARK_HOME=/usr/local/spark-2.4.5
    siwoo@siwoo-ubuntu:~/work/workspace/sparkapp$ $SPARK_HOME/bin/spark-shell
    </pre>

클러스터에서 스파크 실행 @spark-submit
-
개발한 spark application (jars) 을 클러스터 매니저에게 전송해 실행.

* class = 메인 클래스
* master = 클러스터 매니저 url
    
    siwoo@siwoo-ubuntu:~/work/workspace/sparkapp$ mvn clean install
    $SPARK_HOME/bin/spark-submit 
    --class com.siwoo.basic.BasicExample    //main class
    target/sparkapp-1.0-SNAPSHOT.jar  /home/siwoo/work/workspace/sparkapp/src/main/resources //jar & arg

 
메이븐과 스칼라 @scala-maven-plugin
-
메이븐 빌드시 스칼라를 컴파일하는 플로그인.
  
<pre>
    &lt;build&gt;
        &lt;sourceDirectory&gt;src/main/scala&lt;/sourceDirectory&gt;
        &lt;plugins&gt;
            &lt;plugin&gt;
                &lt;groupId&gt;net.alchim31.maven&lt;/groupId&gt;
                &lt;artifactId&gt;scala-maven-plugin&lt;/artifactId&gt;
                &lt;version&gt;3.2.2&lt;/version&gt;
                &lt;executions&gt;
                    &lt;execution&gt;
                        &lt;goals&gt;
                            &lt;goal&gt;compile&lt;/goal&gt;
                            &lt;goal&gt;testCompile&lt;/goal&gt;
                        &lt;/goals&gt;
                    &lt;/execution&gt;
                &lt;/executions&gt;
                &lt;configuration&gt;
                    &lt;scalaVersion&gt;${scala.full.version}&lt;/scalaVersion&gt;
                &lt;/configuration&gt;
            &lt;/plugin&gt;
        &lt;/plugins&gt;
    &lt;/build&gt;
</pre>

구조적 API @structure api
-
DataSet, DataFrame, SQL 테이블과 View

DataFrame    @DataFrame
-
데이터를 row 와 column 으로 표현하는 'untyped' structure api
=> 데이터 타입을 런타임에 결정.
=> Row 타입으로 구성된 DataSet

Column  @column
-
row 에 대한 데이터 타입을 정의와 참조하는 방법을 제공.
* 단순 데이터 타입
* 복합 데이터 타입 


    val column = $"num"
    val df = spark.range(100).toDF("num").select(column)
    
Row @row
-
데이터 레코드.

    val rows: Array[Row] = df.take(10)
    rows.foreach(r => println(r.getAs[Int]("num")))

스파크 데이터 타입 @data type
-
스파크 내부에서 사용하는 데이터 타입.

    val schema = StructType(
                Seq(StructField("num", IntegerType, false), 
                    StructField("even", BooleanType, false)))
Schema   @schema
-
컬럼명과 그 컬럼의 데이터 타입을 정의한 집합.

카탈리스트 엔진    @catalyst engine
-
사용자 코드를 컴파일하여 실행 계획 수립과 최적화를 실행하는 스파크 엔진.  

고급 주제
=
실행 계획   @execution plan
-
구조적 API 은 execution plan 을 제공한다. <br/>
execution plan 은 DAG (directed acyclic graph) 이며, 액션 호출시 생성.  <br/>
각 단계 (transformation) 는 불변성의 DataFrame  을 생성. <br/>
실행 계획은 밑에서 위로 읽으며 디버깅에 유용.

       val df2 = df.groupBy("dest_country_name")
                .agg(sum($"count").as("sums"))
                .orderBy($"sums".desc_nulls_last)

       == Physical Plan ==
       *(3) Sort [sums#66L DESC NULLS LAST], true, 0    // 최종 결과
       +- Exchange rangepartitioning(sums#66L DESC NULLS LAST, 5)   //셔플
          +- *(2) HashAggregate(keys=[dest_country_name#10], functions=[sum(cast(count#12 as bigint))])
             +- Exchange hashpartitioning(dest_country_name#10, 5)
                +- *(1) HashAggregate(keys=[dest_country_name#10], functions=[partial_sum(cast(count#12 as bigint))])
                   +- *(1) FileScan csv [DEST_COUNTRY_NAME#10,count#12] Batched: false, Format: CSV, Location: InMemoryFileIndex[file://..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<DEST_COUNTRY_NAME:string,count:int>
                            //데이터 소스
 
 실행 계획의 과정.
 1. 구조적 api (DataFrame) 을 통해 코드 작성.
 2. Catalyst Optimizer에 의해 논리적 실행 계획 (logical execution plan) 으로 변환. (표현식 컴파일, 컬럼과 테이블 검증)
 3. 물리적 실행 계획 (physical execution plan) 으로 변환후 최적화 (비용 모델 - 테이블의 크기, 파티션 수 - 을 이용한 전략 선택 )
 4. 물리적 실행 계획 (RDD) 이 실행.