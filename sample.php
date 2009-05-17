<?php
include("hadoop.php");


class InitCluster extends Job
{
    function __construct() {
        define("MIN_WORD_LENGTH", 3);
        define("MIN_WORD_FREQ", 3);
    }

    function map($value) {
        if (strpos($value,"||") == 0) {
            return;
        }
        list($url, $text) = explode("||", $value);
        $words = array();
        foreach(preg_split("/[^a-zA-Z]/", $text,0,PREG_SPLIT_NO_EMPTY) as $word) {
            $word = strtolower($word);
            if (strlen($word) < MIN_WORD_LENGTH) {
                continue;
            }
            if (!isset($words[$word])) {
                $words[$word] = 0;
            }
            $words[$word] += 1; 
        }
        foreach ($words as $word => $count) {
            $this->EmitIntermediate($url, "$word,$count");
        }
    }

    function reduce($key, &$values) {
        if (count($values) < MIN_WORD_FREQ) {
            return;
        }
        $this->Emit($key, implode(";", $values));
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
