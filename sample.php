<?php
include("hadoop.php");

class InitCluster extends Job {
    function map($value) {
        if (rand(0,50) == 33) {
            list($key,) = explode("||", $value, 2);
            $key = substr($key, strrpos($key,"=")+1);
            /* reduce the number of possible centroids */
            $this->EmitIntermediate($key, $value );
        }
    }

    function reduce($key, &$values) {
        $this->Emit($key, 1);
    }
}

$hadoop = new Hadoop;
$hadoop->setHome("/home/crodas/hadoop/hadoop-0.18.3");
$hadoop->setJob(new InitCluster);
$hadoop->setInput("noticias/*.txt");
$hadoop->setOutput("noticias/init/");
$hadoop->Run();

?>
