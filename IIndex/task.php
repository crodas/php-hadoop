<?php

final class InvertIndex extends Job
{
    function __config()
    {
        $this->setInput("noticias/*");
        $this->setOutput("index");
        $this->setReducers(2);
    }

    function mapParser($line)
    {
        return explode("||", $line, 2);
    }

    function map($key, &$value)
    {
        $words = array();
        $value = strtolower($value);
        foreach (preg_split("/[^a-z]/i", $value) as $word) {
            $words[$word] = 1;
        }
        foreach ($words as $word=>$id) {
            if (strlen($word) > 2) {
                $this->EmitIntermediate($word, $key);
            }
        }
    }

    function reduce($key, &$values)
    {
        $this->Emit($key, array_unique($values));
    }

}

?>
