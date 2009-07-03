<?php

final class kmeansRandCentroids extends Job
{
    function __config()
    {
        $this->setInput("cluster/data/");
        $this->setOutput("cluster/centroids/");
        $this->setReducers(1);
    }

    function map($key, &$value)
    {
        if (rand(1, 15) == 1) {
            $this->EmitIntermediate($key, $value);
        }
    }

    function reduce($key, &$value)
    {
        static $i=0;
        if ($i++ < KMEANS_CENTROIDS) {
            $this->Emit($i, $value[0]);
        }
    }
}

?>
