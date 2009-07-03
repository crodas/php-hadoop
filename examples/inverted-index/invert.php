<?php
include("../../src/hadoop.php");

include("task.php");


hadoop::setHome("/home/crodas/hadoop/hadoop-0.18.3");

/* run the task */
try {
    new InvertedIndex();
} catch (Exception $e) {
    print $e->getMessage()."\n";
}

?>
