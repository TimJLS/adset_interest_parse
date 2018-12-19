from django.conf.urls import url
from . import views

urlpatterns = [
    url(r'^opt_api/',views.opt_api,name='opt_api'),
]
