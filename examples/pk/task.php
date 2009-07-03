<?php

final class Index extends Job
{
    function __config()
    {
        $this->setInput("news.txt");
        $this->setOutput("pk");
        $this->setOption(HAS_NO_REDUCER);
        $this->setMappers(1);
    }

    function map($key, &$value)
    {
        static $i=0;
        if (trim($key) == "") {
            throw new Exception("Invalid index $key $value");
        }
        $this->EmitIntermediate(++$i, $key);
    }

    function reduce($key, &$values)
    {
    }

}

?>
