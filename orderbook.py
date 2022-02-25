from queue import PriorityQueue
import time
import sys

class Transaction:
    def __init__(self, units, price, id, stockID="xyz"):
        self.units=units
        self.price=price
        self.id = id
        self.stockID=stockID

class orderResponse:
    def __init__(self, status, transactions):
        self.status = status
        self.transactions = transactions


def printBook(buyQueue, sellQueue, cancelledBuy, cancelledSell):
    sellDict = {}
    buyDict = {}
    string = "==========\nASK \n"

    temp1 = PriorityQueue() #make a copy of sellQueue to retain sellQueue
    for order in sellQueue.queue:
        if order[1] in cancelledSell:
            continue
        temp1.put(order)
    while len(temp1.queue):
        a = temp1.get()
        if a[0] not in sellDict:
            sellDict[a[0]] = [a[2]]
        else:
            sellDict[a[0]].append(a[2])
    for key in sellDict:
        string += str(key) + ": "
        for element in sellDict[key]:
            string += str(element) + " "
        string += "\n"

    string += "---------\n"

    temp = PriorityQueue()
    for i in buyQueue.queue:
        if i[1] in cancelledBuy:
            continue
        temp.put(i)
    while len(temp.queue):
        b = temp.get()
        if -b[0] not in buyDict:
            buyDict[-b[0]] = [b[2]]
        else:
            buyDict[-b[0]].append(b[2])
    for key in buyDict:
        string += str(key) + ": "
        for element in buyDict[key]:
            string += str(element) + " "
        string += "\n"

    string += "BID \n==========\n "
    print(string)

def addBuyReq(buyRequest, reqQueue, otherQueue, cancelledSell):
    s = ""
    transactions = []
    while len(otherQueue.queue) and buyRequest.price >= otherQueue.queue[0][0]:
        otherOrder = otherQueue.get()
        if otherOrder[1] in cancelledSell:
            del cancelledSell[otherOrder[1]]
            s += "Trashed a deleted order! [" + str(otherOrder[1]) + "]\n"
            continue
        if buyRequest.units > otherOrder[2]:
            buyRequest.units -= otherOrder[2]
            transactions.append(Transaction(units=otherOrder[2], price=buyRequest.price, id=buyRequest.id, stockID=buyRequest.stockID))
            continue
        else:
            otherOrder[2] -= buyRequest.units
            if otherOrder[2] != 0:
                otherQueue.put(otherOrder)
            transactions.append(Transaction(units=buyRequest.units, price=buyRequest.price, id=buyRequest.id, stockID=buyRequest.stockID))
            break
    else:
        reqQueue.put([-int(buyRequest.price), buyRequest.id, int(buyRequest.units)])

    return orderResponse(status=s, transactions=transactions)

def addSellReq(sellRequest, reqQueue, otherQueue, cancelledBuy):
    s = ""
    transactions = []
    while len(otherQueue.queue) and sellRequest.price <= -otherQueue.queue[0][0]:
        otherOrder = otherQueue.get()
        if otherOrder[1] in cancelledBuy:
            del cancelledBuy[otherOrder[1]]
            s += "Trashed a deleted order! [" + str(otherOrder[1]) + "]\n"
            continue
        if sellRequest.units > otherOrder[2]:
            sellRequest.units -= otherOrder[2]
            transactions.append(Transaction(units=otherOrder[2], price=-otherOrder[0], id=otherOrder[1],  stockID=sellRequest.stockID))
            continue
        else:
            otherOrder[2] -= sellRequest.units
            if otherOrder[2] != 0:
                otherQueue.put(otherOrder)
            transactions.append(Transaction(units=sellRequest.units, price=-otherOrder[0], id=otherOrder[1], stockID=sellRequest.stockID))
            break
    else:
        reqQueue.put([sellRequest.price, sellRequest.id, sellRequest.units]) #log(n) operation only triggered is sell not fulfilled


    return orderResponse(status=s, transactions=transactions)


if __name__ == '__main__':

    start = time.time()
    buyQueue = PriorityQueue() #use a dictionary if expanding to multiple stockID change logic accordingly
    sellQueue = PriorityQueue() #use a dictionary if expanding to multiple stockID change logic accordingly
    cancelledBuy = {} #add an extra layer of nesting if expanding to multiple stockID change logic accordingly
    cancelledSell = {} #add an extra layer of nesting if expanding to multiple stockID change logic accordingly

    #Objects Transaction and Response designed with gRPC req/responses in mind
    #gRPC server/client ommited from submission due to performance cost of serialization/deserialization
    #A response can have multiple transactions embedded
    if len(sys.argv) == 1:
        print("Provide input file!")
        sys.exit(-1)
    with open(sys.argv[1], 'r') as f:
        for line in f:
            items = line.strip().split(",")
            if items[0] == "A":
                if items[2]=='B':
                    resp = addBuyReq(Transaction(int(items[3]),float(items[4]),int(items[1]), "XYZ") , buyQueue, sellQueue, cancelledSell)

                else:
                    resp = addSellReq(Transaction(int(items[3]),float(items[4]),int(items[1]), "XYZ"), sellQueue, buyQueue, cancelledBuy)
            else:
                if items[2] == "B":
                    cancelledBuy[items[1]] = 1
                else:
                    cancelledSell[items[1]] = 1
            if(len(resp.transactions)):
                for transaction in resp.transactions:
                    print(str(transaction.units) + " shares of XYZ were sold at " + str(transaction.price) + " USD")

    print(str(time.time()-start) + "\n")
    printBook(buyQueue, sellQueue, cancelledBuy, cancelledSell)


