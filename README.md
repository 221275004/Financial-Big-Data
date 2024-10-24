第一部分：
要求： 统计数据集上市公司股票代码（“stock”列）的出现次数，按出现次数从⼤到⼩输出，输出格式为"<排名>：<股票代码>，<次数>“
思路：经过观察，stock列为文件analyst_ratings.csv每一行的最后一个逗号内的内容，为什么是最后一个逗号不是第三个逗号？因为在“headline”列中包含标点符号，其中就有逗号，若设置为第三个逗号后，则会提前提取不到stock凑那天目标内容
该部分核心代码如下：
提取部分：
String line = value.toString();   //将value对象转换为字符串并存储在line变量中
int lastCommaIndex = line.lastIndexOf(','); //使用lastIndexOf方法找到字符串中最后一个逗号的位置
if (lastCommaIndex != -1 && lastCommaIndex < line.length() - 1) {  //此处是要保证至少找到一个逗号（lastCommaIndex != -1）以及逗号不是在字符串的最后一个位置（lastCommaIndex < line.length() - 1）
    String extractedContent = line.substring(lastCommaIndex + 1).trim();  //从最后一个逗号之后开始提取字符串，并使用trim()方法去掉前后空格
    word.set(extractedContent);  //将提取到的内容设置为word对象的值
    context.write(word, one);    //将word作为键，one作为值写入上下文,便于接下来的统计次数
}
统计部分：
public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
int sum = 0;
for (IntWritable val : values) {  //这个 for 循环遍历 values 中的每个 IntWritable 对象，val.get() 方法获取当前值的整数值，并将其累加到 sum 中，从而统计单词出现的数量
sum += val.get();
}
countMap.put(key.toString(), sum);
}
输出部分：
protected void cleanup(Context context) throws IOException, InterruptedException {
  PriorityQueue<Map.Entry<String, Integer>> queue = new PriorityQueue<>(  //初始化一个优先队列 queue，每个元素代表一个键值对，键为字符串（stock），值为整数（出现次数）
      (a, b) -> b.getValue().compareTo(a.getValue())  //使用lambda 表达式，定义了队列的排序规则，即根据值进行降序排序
  );
  queue.addAll(countMap.entrySet());
  int rank = 1;
  while (!queue.isEmpty()) {  //遍历队列，输出经过排序后的stock
     Map.Entry<String, Integer> entry = queue.poll();
     context.write(new Text(rank + ":" + entry.getKey() + "，" + entry.getValue()), null);
     rank++;
  }
}
以下是输入命令“hadoop jar /home/njucs/Desktop/work5/stockcount/target/stockcount-1.0-SNAPSHOT.jar com.example.StockCount /user/njucs/analyst_ratings.csv /user/njucs/output”运行程序成功的截图
<img width="736" alt="fe2907164a01c4ad4ea0b4cd9e9c35f" src="https://github.com/user-attachments/assets/b55dba6d-9026-4ac0-90e0-0e986db6e54d">
这个是hdfs dfs -ls /user/njucs/output1/后显示生成的输出文件
<img width="736" alt="fe2907164a01c4ad4ea0b4cd9e9c35f" src="https://github.com/user-attachments/assets/169950d1-3a1c-400c-ae0e-2a358c053626">



第二部分
要求：  统计数据集热点新闻标题（“headline”列）中出现的前100个⾼频单词，按出现次数从⼤到⼩输出。要求忽略⼤⼩写，忽略标点符号，忽略停词（stop-word-list.txt）。输出格式为"<排名>：<单词>，<次数>“
思路：经过观察，“headline”列为文件analyst_ratings.csv每一行的第一个逗号到倒数第二个逗号之间的内容，但是也可以提取第一个逗号到倒数第一个逗号之间，因为程序会忽略数字，倒数第一个逗号与倒数第二个逗号之间为日期，不影响提取单词
该部分核心代码如下：
提取部分：
public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            int firstCommaIndex = line.indexOf(','); //找到第一个逗号的位置
            int lastCommaIndex = line.lastIndexOf(',', line.length() - 2); //找到倒数第二个逗号的位置，但是debug的时候发现这一段还会把日期提取进来，一直找不到问题所在，但是由于日期是数字，所以不影响单词提取

            if (firstCommaIndex != -1 && lastCommaIndex != -1 && firstCommaIndex < lastCommaIndex) {
                String extractedContent = line.substring(firstCommaIndex + 1, lastCommaIndex).trim(); //使用 substring 方法提取出两个逗号之间的内容
                String[] words = extractedContent.toLowerCase().split("\\s+"); //将提取的内容转换为小写，并根据空白字符分割成单词数组

                for (String w : words) {  //遍历上面提取到的单词
                    if (wordPattern.matcher(w).matches()) {
                        String cleanWord = w.toLowerCase();
                        if (!stopWords.contains(cleanWord)) {  //检查单词是否在 stopWords中，如果不在，则将其设置为 word，并将其与计数值one一起写入上下文 context
                            word.set(cleanWord);
                            context.write(word, one);
                        }
                    }
                }
            }
        }
统计部分：和第一部分几乎一摸一样，不多赘述
输出部分：和第一部分几乎一摸一样,但是不是遍历整个数组，而是rank达到100时结束循环，只输出前100个
以下是输入命令“hadoop jar /home/njucs/Desktop/work5/wordcount/target/wordcount-1.0-SNAPSHOT.jar com.example.WordCount /user/njucs/analyst_ratings.csv /user/njucs/output /user/njucs/stop-word-list.txt”运行程序成功的截图
提示：文件的顺序是输入文件 输出文件 停词文件 ，不可改变顺序，不能把停词文件放前面，不然会把它识别为输出文件的路径，会报错
<img width="729" alt="1729735557912" src="https://github.com/user-attachments/assets/4778b4ac-d576-482f-b321-6a20b1ca752e">
这个是hdfs dfs -ls /user/njucs/output/后显示生成的输出文件
<img width="734" alt="1729735593453" src="https://github.com/user-attachments/assets/d0e99c1d-9261-40a7-ab9d-b0f38d481088">
