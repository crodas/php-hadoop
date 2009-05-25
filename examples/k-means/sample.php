<?php
include("../src/hadoop.php");

include("defines.php");
include("prepare.php");
include("random-centroids.php");
include("iteration.php");



hadoop::setHome("/home/crodas/hadoop/hadoop-0.18.3");

exit();

$hadoop = new Hadoop;

/* create an invert index for fast computation */
$hadoop->setInput("noticias/*.txt");
$hadoop->setOutput("noticias/init");
$hadoop->setJob(new initKMeans);
$hadoop->setNumberOfReduces(10);
//$hadoop->Run();

$hadoop->setInput("noticias/init");
$hadoop->setOutput("noticias/centroids");
$hadoop->setJob(new InitCluster);
$hadoop->setNumberOfReduces(1);
//$hadoop->Run();

for($i=1; ;$i++) {
    $hadoop->setInput("noticias/centroids");
    $hadoop->setOutput("noticias/ite-$i/centroids");
    $hadoop->setNumberOfReduces(1);
    $hadoop->setNumberOfMappers(1);
    $hadoop->setJob(new Centroids);
    $hadoop->Run();
    $hadoop = new Hadoop;
    $hadoop->setInput("noticias/init");
    $hadoop->setOutput("noticias/ite-$i/cluster");
    $hadoop->setNumberOfReduces(5);
    $hadoop->setJob(new kmeansIterator);
    $hadoop->Run();
    break;
}


?>
