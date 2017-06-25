"""

"""
import logging

LOG = logging.getLogger(__name__)

try:
    import dbus
    try:
        bus = dbus.SessionBus()
        have_dbus_module = True
    except:
        LOG.exception('Ex1')
        print "warning: dbus is not properly configured, viewer scripting will not be available"
        have_dbus_module = False
        bus = None
except:
    LOG.exception('Ex2')
    print "warning: dbus is not available, viewer scripting will not be available"
    have_dbus_module = False
    bus = None
