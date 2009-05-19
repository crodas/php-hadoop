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

    final private function _getArray(&$arr, $values)
    {
        foreach ($values as $val) {
            list($k, $v) = explode(",", $val, 2);
            $arr[ $k ] = $v;
        }
        ksort($arr);
    }

    function reduce($key, $values)
    {
        if (count($values) < MIN_WORD_FREQ) {
            return;
        }

        $val = array();
        $this->_getArray($val, $values);
        $tmp = array();

        /* some calculations */
        $tmp['sum'] = array_sum($val);
        $tmp['seq'] = array_sum(array_map(array(&$this, "_pearsonpow"), $val));
        $tmp['den'] = $tmp['seq'] - pow($tmp['sum'], 2) / 10000; 

        $values = '';
        foreach ($val as $word => $count) {
            $values .= "$word,$count:";
        }

        $this->Emit($key, implode("|", $tmp) ."|".$values);
    }
}

final class InitCluster extends Job
{
    function __construct()
    {
        define("KMEANS", 10000);

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
        static $i=0;
        if ($i++ < KMEANS) {
            $this->Emit($key, $value[0]);
        }
    }
}

final class ClusterIterator extends Job
{
    function  __construct() 
    {
    }

    function map($line)
    {
    }
    
    function reduce($key, $value)
    {
    }
}


$hadoop = new Hadoop;
/* create an invert index for fast computation */
$hadoop->setHome("/home/crodas/hadoop/hadoop-0.18.3");
$hadoop->setInput("noticias");
$hadoop->setOutput("noticias/init");
$hadoop->setJob(new PrepareCluster);
$hadoop->setNumberOfReduces(4);
$hadoop->Run();


$hadoop->setInput("noticias/init");
$hadoop->setOutput("noticias/step1");
$hadoop->setJob(new InitCluster);
$hadoop->setNumberOfReduces(1);
$hadoop->Run();
die();

for($i=1; ;$i++) {
    $hadoop->setInput("noticias/init");
    $hadoop->setOutput("noticias/ite-$i");
    $hadoop->setNumberOfReduces(5);
    $hadoop->setJob(new ClusterIterator);
    $hadoop->Run();
}
?>
