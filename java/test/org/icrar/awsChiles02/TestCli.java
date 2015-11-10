/**
 *  (c) UWA, The University of Western Australia
 *  M468/35 Stirling Hwy
 *  Perth WA 6009
 *  Australia
 *
 *  Copyright by UWA, 2012-2015
 *  All rights reserved
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 *  This library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  Lesser General Public License for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public
 *  License along with this library; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston,
 *  MA 02111-1307  USA
 */
package org.icrar.awsChiles02;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Test the CLI
 */
public class TestCli {
  private static final Log LOG = LogFactory.getLog(TestCli.class);
  public static void main(String[] args) {
    // create Options object
    Option help = new Option("help", "print this message" );
    Option projecthelp = new Option( "projecthelp", "print project help information" );
    Option version = new Option( "version", "print the version information and exit" );
    Option quiet = new Option( "quiet", "be extra quiet" );
    Option verbose = new Option( "verbose", "be extra verbose" );
    Option debug = new Option( "debug", "print debugging information" );
    Option emacs = new Option( "emacs",
                               "produce logging information without adornments" );

    Option logfile   = Option.builder("logfile" )
                                    .hasArg()
                                    .desc("use given file for log" )
                                    .build();

    Option logger    = Option.builder( "logger" )
                                    .hasArg()
                                    .desc( "the class which it to perform logging" )
                                    .build();

    Option listener  = Option.builder( "listener" )
                                    .hasArg()
                                    .desc("add an instance of class as a project listener" )
                                    .build();

    Option buildfile = Option.builder("buildfile" )
                                    .hasArg()
                                    .desc("use given buildfile" )
                                    .build();

    Option find      = Option.builder( "find" )
                                    .hasArg()
                                    .desc( "search for buildfile towards the root of the filesystem and use it" )
                                    .build();

    Option property  = Option.builder( "D" )
                                    .numberOfArgs(2)
                                    .valueSeparator()
                                    .desc( "use value for given property" )
                                    .build();

    Options options = new Options();

    options.addOption( help );
    options.addOption( projecthelp );
    options.addOption( version );
    options.addOption( quiet );
    options.addOption( verbose );
    options.addOption( debug );
    options.addOption( emacs );
    options.addOption( logfile );
    options.addOption( logger );
    options.addOption( listener );
    options.addOption( buildfile );
    options.addOption( find );
    options.addOption( property );

    // create the parser
    CommandLineParser parser = new DefaultParser();
    try {
      // parse the command line arguments
      CommandLine line = parser.parse(options, args );

      for (String argument : line.getArgList())
        LOG.info(argument);

    }
    catch( ParseException exp ) {
      // oops, something went wrong
      LOG.error( "Parsing failed.", exp);
    }
  }
}
