<?php

final class InitKMeans extends Job
{
    
    function map_parser($line) {
        return explode("||", $line, 2);
    }

    function map($key, &$text)
    {
        $words = array();
        foreach(preg_split("/[^a-zαινσϊόρ]/i", $text,0,PREG_SPLIT_NO_EMPTY) as $word) {
            $word = strtolower($word);
            if (strlen($word) < MIN_WORD_LENGTH) {
                continue;
            }
            if (!isset($words[$word])) {
                $words[$word] = 0;
            }
            $words[$word] += 1; 
        }
        $this->EmitIntermediate($key, $words);
    }

    final static private function _pearsonPow($number)
    {
        return pow($number, 2);
    }

    function reduce($key, &$values)
    {
        $values = $values[0];
        if (count($values) < MIN_WORD_FREQ) {
            return;
        }

        $this->Emit($key, self::initNode($values) );
    }

    public static function initNode(&$values)
    {
        $tmp = new STDClass;

        /* some calculations */
        $tmp->sum = array_sum($values);
        $tmp->seq = array_sum(array_map(array("initKMeans", "_pearsonpow"), $values));
        $tmp->den = $tmp->seq - pow($tmp->sum, 2) / WORD_MATRIX_X; 

        ksort($values);
        $tmp->words = & $values;
        return $tmp;
    }
}

?>
