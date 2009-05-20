<?php

final class ClusterIterator extends Job
{
    private $_centroids;
    private $_centroids_id;

    function  __hadoop_init()
    {
        hadoop::setHome("/home/crodas/hadoop/hadoop-0.18.3");
        $centroids = array();
    
        $fp = Hadoop::getFile("noticias/centroids/part-00000");
        while ($r = fgets($fp)) {
            list($id, $content) = explode("\t", $r, 2);
            $centroids[$id]       = unserialize(trim($content));
            $centroids[$id]->keys = array_keys($centroids[$id]->words);
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

    private function _pearson(stdClass &$obj, stdClass &$centroid)
    {

        $x    = & $obj->words;
        $y    = & $centroid->words;
        $pSum = 0;
        
        foreach ($centroid->keys as $word) {
            if (isset($x[$word])) {
                $pSum += $x[$word] * $y[$word];
            }
        }

        $num = $pSum - ($obj->sum * $centroid->sum / WORD_MATRIX_X);
        $den = sqrt($obj->den * $centroid->den);
        if ($den == 0) {
            return 0;
        }
        return 1-($num/$den);
    }

    function map($key, &$value)
    {
        $centroids    = & $this->_centroids;
        $centroids_id = & $this->_centroids_id;

        $bmatch = 4; 
        $best   = -1;

        foreach ($centroids_id as $id) {
            $dist = $this->_pearson($value, $centroids[$id]);
            if ($dist < $bmatch) {
                $bmatch = $dist;
                $best   = $id;
            }
        }

        if ($bmatch < 0.6) {
            $value->id = $key;
            $this->EmitIntermediate($best, $value);
        }
    }
    
    function reduce($key, &$value)
    {
        $this->Emit($key, $value);
    }
}

?>
