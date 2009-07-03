<?php

function Array_Merge_ex(&$array, $array1)
{
    foreach ($array1 as $key => $value) {
            if (!isset($array[$key])) {
                $array[$key] = 0;
            }
            $array[$key] += $value;
        }
    }

abstract class KmeansBase extends Job
{
    protected $centroids;
    protected $centroids_id;
    protected $threshold = 0.6;

    function __config()
    {
        $this->setInput("cluster/data/");
        $this->setOutput("cluster/ite-1");
        $this->setReducers(2);
        $this->addCache("hdfs://crodas-nb:54310/user/crodas/cluster/centroids/1/part-00000","centroids"); 
    }

    protected function pearson(stdClass &$obj, stdClass &$centroid)
    {

        $x    = & $obj->words;
        $y    = & $centroid->words;
        $keys = & $centroid->keys;
        $pSum = 0;
        
        foreach ($keys as $word) {
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
        $centroids    = & $this->centroids;
        $centroids_id = & $this->centroids_id;
        $threshold    = $this->threshold;

        $bmatch = 4; 
        $best   = -1;

        foreach ($centroids_id as $id) {
            $dist = $this->pearson($value, $centroids[$id]);
            if ($dist < $bmatch) {
                $bmatch = $dist;
                $best   = $id;
            }
        }

        if ($bmatch < $threshold) {
            $value->id = $key;
            $this->EmitIntermediate($best, $value);
        }
    }
    
    function reduce($key, &$values)
    {
        if (count($values) >= 1) {
            $words = array();
            $len   = count($values);
            foreach ($values as $value) {
                array_merge_ex($words, $value->words);
            }
            
            foreach ($words as $word => &$count) {
                $words[$word] = ceil($words[$word]/ $len);
                if ($words[$word] <= 0) {
                    unset($words[$word]);
                }
            }
            $val        = InitKMeans::initNode($words);
            $val->items = & $values;
            $this->Emit($key, $val);
        }
    }

    private function _getItemId($val)
    {
        return $val->id;
    }
}

final class Centroids extends KmeansBase
{
    function  __init()
    {
        $this->threshold = 2;
        $centroids = array();
   
        if (!is_file("centroids")) { 
            throw new Exception("file not found");
        }
        $fp = fopen("centroids","rb");
        $i=0;
        while ($r = fgets($fp)) {
            if (rand(0,5) == 5) {
                list($id, $content) = explode("\t", $r, 2);
                $centroids[$i]       = unserialize(trim($content));
                $centroids[$i]->keys = array_keys($centroids[$i]->words);
                $i++;
            }
        }

        $this->centroids    = & $centroids;
        $this->centroids_id = array_keys($centroids);
        fclose($fp);
    }


}

final class kmeansIterator extends KmeansBase
{
    function  __init()
    {
        $centroids = array();
    
        if (!is_file("centroids")) { 
            throw new Exception("file not found");
        }
        $fp = fopen("centroids","rb");#Hadoop::getFile("noticias/centroids/part-00000");
        while ($r = fgets($fp)) {
            list($id, $content) = explode("\t", $r, 2);
            $centroids[$id]       = unserialize(trim($content));
            $centroids[$id]->keys = array_keys($centroids[$id]->words);

            /*$items    = & $centroids[$id]->items;
            foreach (array_keys($items) as $i) {
                $items[$i]->keys = array_keys($items[$i]->words);
            }*/
        }
        $this->centroids    = & $centroids;
        $this->centroids_id = array_keys($centroids);
        fclose($fp);
    }

    function map($key, &$value)
    {
        $centroids    = & $this->centroids;
        $centroids_id = & $this->centroids_id;
        $threshold    = $this->threshold;

        $bmatch = 4; 
        $best   = -1;

        /* find the great-centroid where it might belong */
        foreach ($centroids_id as $id) {
            $dist = $this->pearson($value, $centroids[$id]);
            if ($dist < $bmatch) {
                $bmatch = $dist;
                $best   = $id;
            }
        }

        /* find the centroid inside the great-centroid where it might belong */
        $_center = & $centroids[$best]->items;
        $len     = count($_center);
        $bmatch  = 4; 
        $best    = -1;
        for ($i=0; $i < $len; $i++) {
            $dist = $this->pearson($value, $_center[$i]);
            if ($dist < $bmatch) {
                $bmatch = $dist;
                $best   = $i;
            }
        }

        if ($bmatch < $threshold) {
            $value->id = $key;
            $this->EmitIntermediate($best, $value);
        }
    }


}
?>
