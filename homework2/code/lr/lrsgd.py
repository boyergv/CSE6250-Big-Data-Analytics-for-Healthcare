# Do not use anything outside of the standard distribution of python
# when implementing this class
import math 

class LogisticRegressionSGD:
    """
    Logistic regression with stochastic gradient descent
    """

    def __init__(self, eta, mu, n_feature):
        """
        Initialization of model parameters
        """
        self.eta = eta
        self.weight = [0.0] * n_feature
        self.mu = mu

    def fit(self, X, y):
        """
        Update model using a pair of training sample
        """
        gamma = math.fsum((self.weight[f]*v for f, v in X))
        if gamma <0:
            sig = (-1.0)*(1.0-1.0/(1.0+math.exp(gamma)))
        else:
            sig = (-1.0)*1.0 / (1.0 + math.exp(-gamma))
        
        for f,v in X:
            self.weight[f] = self.weight[f]+self.eta*(v*sig+y)
            
            
        for idx,val in enumerate(self.weight):
            self.weight[idx] = self.weight[idx]-2*self.eta*self.mu*self.weight[idx]


    def predict_prob(self, X):
        """
        Sigmoid function
        """
        gamma = math.fsum((self.weight[f]*v for f, v in X))
        if gamma <0:
            sig = 1.0-1.0/(1.0+math.exp(gamma))
        else:
            sig = 1.0 / (1.0 + math.exp(-gamma))
        return sig
    
    def predict(self, X):
        """
        Predict 0 or 1 given X and the current weights in the model
        """
        
        return 1 if self.predict_prob(X) > 0.5 else 0