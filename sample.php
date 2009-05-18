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
        foreach(preg_split("/[^a-zαινσϊόρ]/i", $text,0,PREG_SPLIT_NO_EMPTY) as $word) {
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

    final private function _pearsonPow($number)
    {
        return pow($number, 2);
    }

    final private function _getnumber($value) {
        list(, $val) = explode(",", $value);
        return $val;
    }

    function reduce($key, $values) {
        if (count($values) < MIN_WORD_FREQ) {
            return;
        }

        $val = array_map(array(&$this,"_getnumber"), $values);
        $tmp = array();

        $tmp['sum'] = array_sum($val);
        $tmp['seq'] = array_sum(array_map(array(&$this, "_pearsonpow"), $val));
        $tmp['den'] = $tmp['seq'] - pow($tmp['sum'], 2);


        $this->Emit($key, implode("|", $tmp) ."|".implode(":", $values));
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
