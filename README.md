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
<img width="736" alt="fe2907164a01c4ad4ea0b4cd9e9c35f" src="https://github.com/user-attachments/assets/169950d1-3a1c-400c-ae0e-2a358c053626">
