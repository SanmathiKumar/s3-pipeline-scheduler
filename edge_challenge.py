import json
from datetime import datetime
from json import JSONDecodeError
from pathlib import Path
from typing import List, Optional, Union, Dict

from pydantic import BaseModel, ValidationError


class Event(BaseModel):
    class Streaming(BaseModel):
        class LiveStream(BaseModel):
            provider: Optional[str]
            channel: Optional[str]

        class Replay(BaseModel):
            provider: Optional[str]

        livestream: Optional[LiveStream]
        replay: Optional[Replay]

    id: Optional[int]
    raceNumber: Optional[int]
    startTime: Optional[datetime]
    result: Optional[str]
    name: Optional[str]
    sort: Optional[int]
    statusCode: Optional[str]
    streaming: Optional[Streaming]
    streamingAvailable: Optional[bool]
    availableStreamingType: Optional[str]
    mbsAvailable: Optional[bool]
    httpLink: Optional[str]
    regionGroup: Optional[str]
    type: Optional[str]
    category: Optional[str]
    isInternational: Optional[bool]
    distance: Optional[int]
    displayName: Optional[str]


class Pool(BaseModel):
    id: Optional[int]
    poolType: Optional[str]
    eventIds: Optional[List[int]]


class Meeting(BaseModel):
    id: Optional[int]
    name: Optional[str]
    classId: Optional[int]
    isInternational: Optional[bool]
    className: Optional[str]
    regionName: Optional[str]
    streamingAvailable: Optional[bool]
    availableStreamingType: Optional[str]
    mbsAvailable: Optional[bool]
    events: Optional[List[Event]] = [Event()]
    pools: Optional[List[Pool]] = [Pool()]
    streaming: Optional[Event.Streaming]


class Section(BaseModel):
    displayName: Optional[str]
    displayOrder: Optional[int]
    raceType: Optional[str]
    meetings: Optional[List[Meeting]] = [Meeting()]


class Date(BaseModel):
    meetingDate: Optional[datetime]
    sections: Optional[List[Section]] = [Section()]


class Response(BaseModel):
    dates: Optional[List[Date]] = [Date()]


def parse_data(input_file: Union[Path, str]) -> List[Dict]:
    try:
        response_raw = json.load(open(input_file, "r"))
        response = Response(**response_raw)
        races_ = list()
        for date in response.dates:
            for section in date.sections:
                for meeting in section.meetings:
                    if meeting is not None:
                        for event in meeting.events:
                            race = {
                                "meeting_id": meeting.id,
                                "meeting_name": meeting.name,
                                "race_number": event.raceNumber,
                                "race_link": event.httpLink,
                                "event_id": event.id,
                                "distance": event.distance,
                                "start_time": event.startTime.isoformat(),
                            }
                            races_.append(race)
        return races_
    except (ValidationError, JSONDecodeError, UnicodeDecodeError):
        print(f"Cannot parse the file: `{path.as_posix()}`. Invalid format.")
        raise


if __name__ == "__main__":
    path = Path(input("Enter file location needed for parsing: "))
    save_path = path.parent / f"{path.stem}_parsed{path.suffix}"
    if path.exists():
        try:
            races = parse_data(path)
            json.dump(
                races,
                open(save_path, "w"),
                indent=4,
            )
            print(
                f"Found {len(races)} races. \nSaved parsed file at: `{save_path.as_posix()}`"
            )
        except Exception as e:
            pass
    else:
        raise FileNotFoundError(
            f"The specified path does not exist. Please correct it and try again."
            f"`{path.as_posix()}`"
        )