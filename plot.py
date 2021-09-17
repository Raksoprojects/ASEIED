from matplotlib import pyplot as plt


if __name__ == '__main__':

    avg = {'Green_taxi19': 2.7606352972639128, 'Yellow_taxi19': 2.487639197232751, 'Green_taxi20': 21.230380260467584, 'Yellow_taxi20': 6.337120249430919}

    plt.bar(range(len(avg)), list(avg.values()), align='center')
    plt.xticks(range(len(avg)), list(avg.keys()))

    plt.show()