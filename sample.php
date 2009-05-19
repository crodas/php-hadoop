<?php
include("src/hadoop.php");


final class PrepareCluster extends Job
{
    function __hadoop_init()
    {
        define("MIN_WORD_LENGTH", 3);
        define("MIN_WORD_FREQ", 3);
        define("WORD_MATRIX_X", 5000);
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
        $tmp['den'] = $tmp['seq'] - pow($tmp['sum'], 2) / WORD_MATRIX_X; 

        $values = '';
        foreach ($val as $word => $count) {
            $values .= "$word,$count:";
        }

        $this->Emit($key, implode("|", $tmp) ."|".$values);
    }
}

final class InitCluster extends Job
{
    function __hadoop_init()
    {
            define("KMEANS", 4000);

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
            $this->Emit($i, $value[0]);
        }
    }
}

final class ClusterIterator extends Job
{
    private $_centroids;
    private $_centroids_id;
    function  __hadoop_init()
    {
        hadoop::setHome("/home/crodas/hadoop/hadoop-0.18.3");
        define("WORD_MATRIX_X", 5000);
        $centroids = array();
    
        $fp = Hadoop::getFile("noticias/centroids/part-00000");
        while ($r = fgets($fp)) {
            list($id, $content) = explode("\t", $r, 2);
            $centroids[$id] = $this->_getObject($content);
        }
        $this->_centroids    = & $centroids;
        $this->_centroids_id = array_keys($centroids);
        fclose($fp);
    }

    private function _getObject($line)
    {
        $obj = new stdClass;

        list($s, $seq, $den, $extra) = explode("|", $line, 4);
        $obj->sum   = $s;
        $obj->seq   = $seq;
        $obj->den   = $den;
        $obj->words = & $words;
        $words      = array();
        foreach (explode(":", $extra) as $word) {
            if (trim($word) == "") {
                continue;
            }
            list($w, $c) = explode(",", $word, 2);
            $words[$w] = $c;
        }

        return $obj;
    }

    private function _pearson(&$obj, &$centroid)
    {

        $x    = & $obj->words;
        $y    = & $centroid->words;
        $pSum = 0;
        /*reset($x); reset($y);
        while (current($x) && current($y)) {
            $cmp = strcmp(key($x), key($y));
            if ($cmp < 0) {
                next($x);
            } else if ($cmp == 0) {
                $pSum += current($x) * current($y);
                next($x); next($y);
            } else {
                next($y);
            }
        }*/
        foreach ($x as $word=>$cnt) {
            if (isset($y[$word])) {
                $pSum += $cnt * $y[$word];
            }
        }

        $num = $pSum - ($obj->sum * $centroid->sum / WORD_MATRIX_X);
        $den = sqrt($obj->den * $centroid->den);
        if ($den == 0) {
            return 0;
        }
        return 1-($num/$den);
    }

    function map($line)
    {
        $centroids    = & $this->_centroids;
        $centroids_id = & $this->_centroids_id;

        list($key, $content) = explode("\t", $line, 2);
        $word   = $this->_getObject($content);
        $bmatch = 4; 
        $best   = -1;

        foreach ($centroids_id as $id) {
            $dist = $this->_pearson($word, $centroids[$id]);
            if ($dist < $bmatch) {
                $bmatch = $dist;
                $best   = $id;
            }
        }

       fwrite(STDERR, "$key $best $bmatch\n");
        if ($bmatch < 0.6) {
            fwrite(STDERR, "$key $best $bmatch\n");
            $this->EmitIntermediate($key, $best);
        }
    }
    
    function reduce($key, $value)
    {
        $this->Emit($key, $value[0]);
    }
}

hadoop::setHome("/home/crodas/hadoop/hadoop-0.18.3");

$hadoop = new Hadoop;
/* create an invert index for fast computation */
$hadoop->setInput("noticias/*.txt");
$hadoop->setOutput("noticias/init");
$hadoop->setJob(new PrepareCluster);
$hadoop->setNumberOfReduces(10);
//$hadoop->Run();


$hadoop->setInput("noticias/init");
$hadoop->setOutput("noticias/centroids");
$hadoop->setJob(new InitCluster);
$hadoop->setNumberOfReduces(1);
//$hadoop->Run();

for($i=1; ;$i++) {
    $hadoop->setInput("noticias/init");
    $hadoop->setOutput("noticias/ite-$i");
    $hadoop->setNumberOfReduces(1);
    $hadoop->setJob(new ClusterIterator);
    $hadoop->Run();
    break;
}
?>
