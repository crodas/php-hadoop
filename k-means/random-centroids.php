<?php

final class InitCluster extends Job
{
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
