补充一下运行命令。由于在写README时的图片文件以临时文件存储，想到应该补充命令操作的时候已经将电脑更新了一个版本，相关图片文件被删除了，无法重置README.pdf。为了保持美观，以REDAME2的方式记录。

`bin/hadoop jar wc3.jar input/ output/`

注意后的input和output的斜杠不能省略，停用词、大小写敏感等其他参数已经内置在jar包的代码内，其中停用词文件punctuation.txt和stop-word-list.txt需设置放在input同级目录下的stop文件夹中。这些实际上都可以修缮的更加人性化和灵活个性化，由于完成时有些赶，就没有顾得上这些。

另：在后续和同学的交流中，了解到可以在Map阶段先把文本名附在键（单词）上，再在Reduce阶段依据文本名分流到不同的文件中，这样只需要进行一个MapReduce工作，相对来说更省资源一些；在自己写的代码中，想法是遍历input下的所有文本，每次都作为一个特定的MR任务输出到特定文件夹的文件中，虽说借此也熟悉了一下HDFS的接口，但确实不是在个人PC上合适的运行方式。