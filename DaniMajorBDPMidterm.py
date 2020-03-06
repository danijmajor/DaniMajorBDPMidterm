from pyspark import SparkConf, SparkContext
import aspell

conf = SparkConf().setMaster('local').setAppName('DMajorBDPMidterm.py')
sc = SparkContext(conf = conf)

#import files
fileToAnalyze1 = sc.textFile('/home/dani/Downloads/Encrypted-1.txt')
fileToAnalyze1.take(1)

fileToAnalyze2 = sc.textFile('/home/dani/Downloads/Encrypted-2.txt')
fileToAnalyze2.take(1)

fileToAnalyze3 = sc.textFile('/home/dani/Downloads/Encrypted-3.txt')
fileToAnalyze3.take(1)


def get_words(name):
    #separate words
    global words
    words = name.map(lambda line: str(line)).flatMap(lambda line: line.split())
    words.take(1)
    #get total number of words
    words_count = words.map(lambda word: (word, 1)).reduceByKey(lambda x, y: x + y)
    total_words = sum(words_count.collectAsMap().values())
    print("The word count for this document is : " + str(total_words))
    #get most used words
    words_frequencies = words_count.top(50, lambda x: x[1])
    words_top = str(words_frequencies[0])
    print("The most common word frequency for this file is : " + words_top)


def get_chars(name):
    #separate characters
    global chars
    chars = name.flatMap(lambda line: line)
    chars.take(5)
    #get total number of chars
    char_count = chars.map(lambda char: (char, 1)).reduceByKey(lambda x, y: x + y)
    chars_counts = char_count.collectAsMap()
    total_chars = sum(chars_counts.values())
    #get most used characters
    char_frequencies = char_count.top(50, lambda x: x[1])
    if "Encrypted-1.txt" not in str(name):
        top_char = str(char_frequencies[1])
    else:
        top_char = str(char_frequencies[2])
    global s_val
    s_val = top_char[3]
    if "Encrypted-1.txt" in str(name):
        txt = "Encrypted-1.txt"
    elif "Encrypted-2.txt" in str(name):
        txt = "Encrypted-2.txt"
    elif "Encrypted-3.txt" in str(name):
        txt = "Encrypted-3.txt"
    print(txt)
    print("The character count for this document is : " + str(total_chars))
    print("The most common character frequency for this file is : " + top_char)

letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"


def get_shift(s):
    global shift
    shift = letters.find(s) - letters.find("E")
    print("The shift of the Cypher for the file is : " + str(shift))
    print("In this file, common English character 'E', is represented by : " + s)
    return shift


def decrypt(ss, shift):
    return "".join([
        letters[(letters.find(s) - shift) % len(letters)]
        if s in letters else s
        for s in ss
    ])


#initalize speller for each partition
def check_words(words):
    a = aspell.Speller('lang', 'en')
    for word in words:
        yield a.check(word)


def translate(name, path):
    decrypted = words.map(lambda w: decrypt(w, shift=shift))
    #find the fraction of decrypted words that are in the dictionary
    decrypted.map(check_words).mean()
    #save as text file
    name.coalesce(1).map(lambda line: decrypt(line, shift)).saveAsTextFile(path)


def output(name, path):
    get_chars(name)
    get_words(name)
    get_shift(s_val)
    translate(name, path)
    print(" ")

output(fileToAnalyze1,"/tmp/output1/")
output(fileToAnalyze2,"/tmp/output2/")
output(fileToAnalyze3,"/tmp/output3/")
