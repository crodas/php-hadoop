<?php
include("hadoop.php");

class InitCluster extends Job {
    function map($key, $value) {
        if (rand(0,1) == 1) {
            /* reduce the number of possible centroids */
            $this->EmitIntermediate($key, serialize($value) );
        }
    }

    function reduce($key) {
    }
}

$hadoop = new Hadoop;
$hadoop->setHome("/home/crodas/hadoop/hadoop-0.18.3");
$hadoop->setJob(new InitCluster);
$hadoop->setInput("noticias/*.txt");
$hadoop->setOutput("noticias/init/");
$hadoop->Run();

?>
