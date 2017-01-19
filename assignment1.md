Assignment 1
=====================
Question 1.
-----------------
Pair implementation includes 2 jobs. Mapper in the 1st job maps a text line into KVP of words(also the line itself, denoted as "*" ) and 1's. Reducer sums up the counts by word, output is also text to integers. The second job first maps each text lines into KVP of word pairs and 1's. The reducer first read the result of previous job, put the result into a hashtable in the setup phase. Then aggregate the result from intermediate KVP (pairOfWords, count). Then get the count of each word from word count hashtable and number of lines from setup phase. Then use the 4 numbers to calculate PMI. Write result as KVP firstWord  and map of secondWord to (pmi, co-occurrence) pair to disk.

Stripes implementation includes 2 jobs. The 1st job is identical to pair implementation. The second job first maps each text lines into KVP of word to map of words to counts. The reducer first read the result of previous job, put the result into a hashtable in the setup phase. Then aggregate the result from intermediate KVP (words, words to counts map). Then get the count of each word from word count hashtable and number of lines from setup phase. Then use the 4 numbers to calculate PMI. Write result as KVP firstWord  and map of secondWord to (pmi, co-occurrence) pair to disk.

Question 2.
------------
it takes 50.696s to run PairPMI on student.cs.linux.uwaterloo.ca machine.
it takes 16.652s to run StripesPMI on student.cs.linux.uwaterloo.ca machine.

Question 3.
---------------
Without "-imc",
It takes 57.638s to run PairPMI on student.cs.linux.uwaterloo.ca machine.
It takes 16.569s to run StripesPMI on student.cs.linux.uwaterloo.ca machine.



Question 4.
-------------------------
   77198  308792 2327802


Question 5. (highest PMI)
--------------------
(maine, anjou)	(3.6331422, 12)

(anjou, maine)	(3.6331422, 12)


Question 5. (lowest PMI)
-----------------------------
(thy, you)	(-1.5303968, 11)

(you, thy)	(-1.5303968, 11)


Question 6. ('tears')
-----------------------
(tears, shed)	(2.1117902, 15)

(tears, salt)	(2.0528123, 11)

(tears, eyes)	(1.165167, 23)


Question 6. ('death')
-----------------------------
(death, father's)	(1.120252, 21)

(death, die)	(0.7541594, 18)

(death, life)	(0.7381346, 31)


Question 7.
--------
(hockey, defenceman)    (2.4030268, 147)

(hockey, winger)    (2.3863757, 185)

(hockey, goaltender)    (2.2434428, 198)

(hockey, ice)   (2.195185, 2002)

(hockey, nhl)   (1.9864639, 940)


Question 8.
------------
(data, storage) (1.9796829, 100)

(data, database)    (1.8992721, 97)

(data, disk)    (1.7935462, 67)

(data, stored)  (1.7868549, 65)

(data, processing)  (1.6476576, 57)
