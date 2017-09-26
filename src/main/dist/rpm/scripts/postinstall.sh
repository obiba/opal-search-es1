#!/bin/sh
# postinst script for opal-search-es
#

set -e

# summary of how this script can be called:
#        * <postinst> `configure' <most-recently-configured-version>
#        * <old-postinst> `abort-upgrade' <new version>
#        * <conflictor's-postinst> `abort-remove' `in-favour' <package>
#          <new-version>
#        * <postinst> `abort-remove'
#        * <deconfigured's-postinst> `abort-deconfigure' `in-favour'
#          <failed-install-package> <version> `removing'
#          <conflicting-package> <version>
# for details, see http://www.debian.org/doc/debian-policy/ or
# the debian-policy package

NAME=opal-search-es

[ -r /etc/default/$NAME ] && . /etc/default/$NAME

case "$1" in
  [1-2])

      if [ -f /etc/default/opal ]; then
        . /etc/default/opal
        mkdir -p $OPAL_HOME/plugins
        if [ -d "$OPAL_HOME"/plugins ]; then

          OLD_PLUGIN=$(ls -t "$OPAL_HOME"/plugins/ | grep opal-search-es | head -1)
          NEW_PLUGIN=$(ls -t /usr/share/opal-search-es/ | grep opal-search-es | head -1 | sed s/\-dist\.zip//g)
          NEW_PLUGIN_ZIP="$NEW_PLUGIN-dist.zip"

          unzip /usr/share/opal-search-es/$NEW_PLUGIN_ZIP -d $OPAL_HOME/plugins/
          touch $OPAL_HOME/plugins/$NEW_PLUGIN

          if [ ! -z "$OLD_PLUGIN" ] && [ -f $OPAL_HOME/plugins/$OLD_PLUGIN/site.properties ]; then
            echo "Copying $OLD_PLUGIN/site.properties to new installation."
            cp $OPAL_HOME/plugins/$OLD_PLUGIN/site.properties $OPAL_HOME/plugins/$NEW_PLUGIN/
          fi

          chown -R opal:adm $OPAL_HOME/plugins/$NEW_PLUGIN
          echo '***'
          echo '*** IMPORTANT: Opal Search ES plugin has been installed, you must restart Opal server.'
          echo '***'
        fi
      fi

  ;;

  *)
    echo "postinst called with unknown argument \`$1'" >&2
    exit 1
  ;;
esac

exit 0
