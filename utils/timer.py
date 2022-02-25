import time

def RateLimited(maxPerSecond):
    minInterval = 1.0 / float(maxPerSecond)
    def decorate(func):
        lastTimeCalled = [0.0]
        def rateLimitedFunction(*args,**kargs):
            elapsed = time.time() - lastTimeCalled[0]
            leftToWait = minInterval - elapsed
            if leftToWait>0:
                print("leftToWait: ", leftToWait)
                time.sleep(leftToWait)
                
            ret = func(*args,**kargs)
            lastTimeCalled[0] = time.time()
            return ret
        return rateLimitedFunction
    return decorate

'''
Example: 
@RateLimited(25)  # 25 per second at most
def PrintNumber(num):
    print(num)

if __name__ == "__main__":
    print("This should print 1,2,3... at about 2 per second.")
    for i in range(1,1500):
        PrintNumber(i)
'''