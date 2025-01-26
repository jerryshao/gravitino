package org.apache.gravitino.file;

import org.apache.gravitino.annotation.Evolving;

import java.util.List;

@Evolving
public interface SupportsSync {

  JobHandle sync(String srcLocation, List<String> destLocations);

}
