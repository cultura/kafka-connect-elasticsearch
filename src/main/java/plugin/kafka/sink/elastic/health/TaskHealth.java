/*
    Copyright 2019 Cultura (SOCULTUR).

    This file is part of kafka-connect-elasticsearch.

    kafka-connect-elasticsearch is free software: you can redistribute it and/or modify
    it under the terms of the GNU Lesser General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    kafka-connect-elasticsearch is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Lesser General Public License for more details.

    You should have received a copy of the GNU Lesser General Public License
    along with kafka-connect-elasticsearch.  If not, see <https://www.gnu.org/licenses/>.

    contact: team.api.support@cultura.fr
 */

package plugin.kafka.sink.elastic.health;

import lombok.Getter;
import lombok.Setter;
import org.apache.kafka.connect.errors.ConnectException;

/**
 * Keep the first unrecoverable exception thrown in the task instance.
 */
@Getter
@Setter
public class TaskHealth {

  private ConnectException exception;

  public void throwIfException() {
    if (exception != null) {
      throw this.exception;
    }
  }

}
