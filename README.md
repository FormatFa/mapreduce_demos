hadoop mapreduce 例子收集

### 1. 寻找共同好友
>给出A-O个人中每个人的好友列表，求出哪些人两两之间有共同好友，以及他们的共同好友都有谁

1. 原始数据
```
A:B,C,D,F,E,O
B:A,C,E,K
C:F,A,D,I
D:A,E,F,L
E:B,C,D,M,L
F:A,B,C,D,E,O,M
G:A,C,D,E,F
H:A,C,D,E,O
I:A,O
J:B,O
K:A,C,D
L:D,E,F
M:E,F,G
O:A,H,I,J
```
2. 输出结果
```$xslt
A-B	E,C
A-C	D,F
A-D	E,F
```

3.理解
- 原始数据

某个用户a   a用户的好友列表

- 第一步转换

某个用户a   有a用户的好友列表l2 [b,c,d,f...]

- 第二步

l2 都有a用户这个好友，l2里的用户随便两两组合就，a就是他们的共同好友
 
b-c ,a,d,f...  


参考
- https://blog.csdn.net/u012808902/article/details/77513188