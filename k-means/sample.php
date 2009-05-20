<?php
include("../src/hadoop.php");

include("defines.php");
include("prepare.php");
include("random-centroids.php");
include("iteration.php");



hadoop::setHome("/home/crodas/hadoop/hadoop-0.18.3");

$hadoop = new Hadoop;

/* create an invert index for fast computation */
$hadoop->setInput("noticias/*.txt");
$hadoop->setOutput("noticias/init");
$hadoop->setJob(new PrepareCluster);
$hadoop->setNumberOfReduces(2);
$hadoop->Run();

$hadoop->setInput("noticias/init");
$hadoop->setOutput("noticias/centroids");
$hadoop->setJob(new InitCluster);
$hadoop->setNumberOfReduces(1);
$hadoop->Run();
exit();

for($i=1; ;$i++) {
    $hadoop->setInput("noticias/init");
    $hadoop->setOutput("noticias/ite-$i");
    $hadoop->setNumberOfReduces(1);
    $hadoop->setJob(new ClusterIterator);
    $hadoop->Run();
    break;
}


?>
