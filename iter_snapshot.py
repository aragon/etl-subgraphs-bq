from main_subgraph import main

accum = 0
while accum < 1651831124:
    accum = main()
    print("---accum: ",accum)
