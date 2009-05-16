<?php
include("hadoop.php");


class InitCluster extends Job {
    function __construct() {
        define("MIN_WORD_LENGTH", 3);
        define("MIN_WORD_FREQ", 2);
    }
    function map($value) {
        if (strpos($value,"||") == 0) {
            return;
        }
        list($url, $words) = explode("||", $value);
        if (strpos($url, "?pid=") !== false) {
            $docid = "abc". substr($url, strrpos($url, "=") + 1);
        } else {
            $docid = "ln". substr($url, strrpos($url, "=") + 1);
        }
        if (!isset($docid) || $docid == "") {
            return;
        }
        $docid = $url;
        foreach(preg_split("/[^a-zA-Z]/", $words,0,PREG_SPLIT_NO_EMPTY) as $word) {
            if (strlen($word) < MIN_WORD_LENGTH) {
                continue;
            }
            $this->EmitIntermediate(strtolower($word), $docid);
        }
    }

    function reduce($key, &$values) {
        if (count($values) < MIN_WORD_FREQ) {
            return;
        }
        $this->Emit($key, implode("|", $values));
    }
}

$hadoop = new Hadoop;
/* create an invert index for fast computation */
$hadoop->setHome("/home/crodas/hadoop/hadoop-0.18.3");
$hadoop->setJob(new InitCluster);
$hadoop->setInput("noticias/*.txt");
$hadoop->setOutput("noticias/init");
$hadoop->Run();

?>
