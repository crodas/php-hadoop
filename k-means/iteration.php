<?php

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
        
        /*`reset($x); reset($y);
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

    function map($key, &$line)
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

        if ($bmatch < 0.6) {
            //fwrite(STDERR, "$key $best $bmatch\n");
            $values = '';
            foreach ($word->words as $k => $count) {
                $values .= "$k,$count:";
            }
            $this->EmitIntermediate($key, $best."|".$values);
        }
    }
    
    function reduce($key, &$value)
    {
        $this->Emit($key, $value[0]);
    }
}

?>
