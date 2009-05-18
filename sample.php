<?php
include("src/hadoop.php");


final class PrepareCluster extends Job
{
    function __construct()
    {
        define("MIN_WORD_LENGTH", 3);
        define("MIN_WORD_FREQ", 3);
    }

    function map($value)
    {
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

    final private function _getnumber($value)
    {
        list(, $val) = explode(",", $value);
        return $val;
    }

    function reduce($key, $values)
    {
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

final class InitCluster extends Job
{
    function __construct()
    {
        global $i;
        $i = 0;
        define("KMEANS", 1000);

    }

    function map($value)
    {
        if (rand(1, 10) == 1) {
            list($url,$value) = explode("\t", $value);
            $this->EmitIntermediate($url, $value);
        }
    }

    function reduce($key, $value)
    {
        global $i;
        if ($i++ > KMEANS) {
            $this->Emit($key, $value[0]);
        }
    }
}

$hadoop = new Hadoop;
/* create an invert index for fast computation */
$hadoop->setHome("/home/crodas/hadoop/hadoop-0.18.3");
$hadoop->setInput("noticias/*.txt");
$hadoop->setOutput("noticias/init");
$hadoop->setJob(new PrepareCluster);
//$hadoop->Run();


$hadoop->setInput("noticias/init");
$hadoop->setOutput("noticias/step1");
$hadoop->setJob(new InitCluster);
$hadoop->Run();

?>
