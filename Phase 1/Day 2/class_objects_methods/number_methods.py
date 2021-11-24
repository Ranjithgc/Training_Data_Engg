def factorial(n):
    fact = 1
    for num in range(2, n + 1):
        fact *= num
    return fact

def fibonacci(n):
    a = 0 
    b = 1
    c = 0
    while(n-2):
        c = a + b
        a = b
        b = c
        print(c, end=" ")
        n = n - 1

def evenOrodd(num):
    if(num % 2 == 0):
        print("even number")
    else:
        print("odd number")

def prime(num):
    flag = False

# prime numbers are greater than 1
    if num > 1:
    # check for factors
        for i in range(2, num):
            if (num % i) == 0:
            # if factor is found, set flag to True
                flag = True
            # break out of loop
            break

# check if flag is True
    if flag:
        print(num, "is not a prime number")
    else:
        print(num, "is a prime number")


print(factorial(12))
fibonacci(10)

num = int(input("enter the number"))
evenOrodd(num)

prime(12)