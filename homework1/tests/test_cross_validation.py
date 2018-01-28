from nose.tools import with_setup, ok_, eq_, assert_almost_equals, nottest
from src.utils import get_data_from_svmlight
from src.models_partc import logistic_regression_pred,svm_pred,decisionTree_pred,classification_metrics
from src.cross import get_acc_auc_kfold

def test_auc_cv():
	expected = 0.707577330303
	X,Y = get_data_from_svmlight("deliverables/features_svmlight.train")
	actual = get_acc_auc_kfold(X,Y)[1]
	assert_almost_equals(expected, actual,places=1, msg="UNEQUAL Expected:%s, Actual:%s" %(expected, actual))
