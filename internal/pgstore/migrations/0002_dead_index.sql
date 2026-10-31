-- ListDeadJobs pages by (finished_at, id); with id in the index, every page
-- is an index range scan, including the tiebreak between equal finished_at.
DROP INDEX jobs_dead;
CREATE INDEX jobs_dead ON jobs (queue, finished_at, id) WHERE state = 'dead';
