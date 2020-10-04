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

DataFrame    @DataFrame
-
데이터를 row 와 column 으로 표현하는 structure api
