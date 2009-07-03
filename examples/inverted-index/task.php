<?php

/**
 *  Invert Index
 *
 *  This class contains an Invert Index.
 */
final class InvertedIndex extends Job
{
    function __config()
    {
        $this->setInput("news.txt");
        $this->setOutput("inverted-index");
        $this->setMappers(4);
        $this->setReducers(2);
    }

    function map($key, &$value)
    {
        $words = array();
        $value = strtolower($value);
        foreach (preg_split("/[^a-zαινσϊόρ]/i", $value) as $word) {
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
