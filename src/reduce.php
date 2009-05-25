#!/usr/bin/php
<?
/*include*/

/*class*/
hadoop::initReducer();
hadoop::setHome("/*hadoop-home*/");

$map = new /*name*/;
$map->RunReduce();

?>
