class Config:
    # loss_function = 'huber'
    # batch_size = 64

    def __init__(self, loss_function='huber', batch_size=64, load_path='ML/data/clean.csv'):
        self.loss_function = loss_function
        self.batch_size = batch_size
        self.load_path = load_path