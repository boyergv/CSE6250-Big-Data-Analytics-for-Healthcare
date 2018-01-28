from nose.tools import with_setup, ok_, eq_, assert_almost_equals, nottest
from src.utils import get_data_from_svmlight
from src.models_partb import logistic_regression_pred,svm_pred,decisionTree_pred,classification_metrics
from src.cross import get_acc_auc_kfold

def setup_module ():
    global X_train, Y_train
    X_train, Y_train = get_data_from_svmlight("deliverables/features_svmlight.train")

def test_accuracy_lr():
	expected = 0.954545454545
	Y_pred = logistic_regression_pred(X_train,Y_train)
	actual = classification_metrics(Y_pred,Y_train)[0]
	assert_almost_equals(expected, actual,places=2, msg="UNEQUAL Expected:%s, Actual:%s" %(expected, actual))

def test_auc_svm():
	expected = 0.994511904762
	Y_pred = svm_pred(X_train,Y_train)
	actual = classification_metrics(Y_pred,Y_train)[1]
	assert_almost_equals(expected, actual,places=1, msg="UNEQUAL Expected:%s, Actual:%s" %(expected, actual))

def test_fscore_dt():
	expected = 0.68358714044
	Y_pred = decisionTree_pred(X_train,Y_train)
	actual = classification_metrics(Y_pred,Y_train)[4]
	assert_almost_equals(expected, actual,places=2, msg="UNEQUAL Expected:%s, Actual:%s" %(expected, actual))
