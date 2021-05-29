row_len={
    20:'80M',
    100:'60M',
    200:'30M',
    400:'15M',
    600:'5M',
    999:'1M'}

shard={
    999:'2M',
    400:'5M',
    200:'20M',
    100:'50M',
    20:'100M'
    }


r=26

row_count=458963694
for key , val in row_len.items():
    if key>r:
        chunk=int(row_len[key].replace('M',''))*1000000
        break



def setshard( rowcount, rowlen):
        try :
            shard,chunk=0,0
            if int(rowcount) > 1000000000:
                shard = 10000000
            elif (int(rowcount) > 500000000 and int(rowcount) < 1000000000):
                shard = 9000000            
            elif (int(rowcount) > 400000000 and int(rowcount) <= 500000000):
                shard = 8000000
            elif (int(rowcount) > 300000000 and int(rowcount) <= 400000000):
                shard = 7000000
            elif (int(rowcount) > 200000000 and int(rowcount) <= 300000000):
                shard = 6000000
            elif (int(rowcount) > 100000000 and int(rowcount) <= 200000000):
                shard = 6000000
            elif (int(rowcount) > 5000000 and int(rowcount) <= 100000000):
                shard = 6000000
            elif (int(rowcount) > 1000000 and int(rowcount) <= 5000000):
                shard = 6000000
            elif (int(rowcount) <= 1000000):
                shard = 8000000
            else:
                shard = 8000000

            if int(rowlen)>999:
                chunk=100000
            elif int(rowlen)>700 and int(rowlen)<=999:
                chunk=150000
            elif int(rowlen)>400 and int(rowlen)<=700:
                chunk=200000
            elif int(rowlen)>300 and int(rowlen)<=400:
                chunk=300000
            elif int(rowlen)>200 and int(rowlen)<=300:
                chunk=500000
            elif int(rowlen)>50 and int(rowlen)<=200:
                chunk=900000
            else:
                chunk=500000
            return shard,chunk
        except Exception as error :
            print ("Error in setshard : " +str(error))


print(setshard( 45693156, 425))
