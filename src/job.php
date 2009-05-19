<?php

abstract class Job
{
    final function EmitIntermediate($key, $value)
    {
        printf("%s\t%s\n",$key, $value);
    }

    final function Emit($key, $value)
    {
        printf("%s\t%s\n",$key, $value);
    }

    final function RunMap()
    {
        $this->__hadoop_init();
        while (($line = fgets(STDIN)) !== false) {
            $line = substr($line, 0, strlen($line)-1);
            if (strlen($line) == 0) {
                continue;
            }
            $this->map($line);
        }

    }

    final function RunReduce()
    {
        $this->__hadoop_init();
        $values = new parray();
 
        while (($line = fgets(STDIN)) !== false) {
            $key = $value = null;
            list($key, $value) = explode("\t", $line, 2);
            if ($key===null || $value===null) {
                continue;
            }
            $values->$key = substr($value,0,strlen($value) -1);
        }

        foreach ($values->getKeys() as $id) {
            if (count($values->$id) == 0) {
                continue;
            }
            $this->reduce($id, $values->$id);
        }
        $values = null;
    }

    abstract protected function map($value);
    abstract protected function reduce($key, $iterable);
}

?>
