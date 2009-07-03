<?php
include("../../src/hadoop.php");

include("defines.php");
include("prepare.php");
include("random-centroids.php");
include("iteration.php");



hadoop::setHome("/home/crodas/hadoop/hadoop-0.18.3");

try {
    new kmeansInit();
} catch (Exception $e) { 
    #
}

try {
    new kmeansRandCentroids();
} catch (Exception $e) {
}

new Centroids();
new kmeansIterator();

?>
