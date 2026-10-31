-- ListDeadJobs pages by (finished_at, id); with id in the index, every page
-- is an index range scan, including the tiebreak between equal finished_at.
-- Build the new index before dropping the old one: CREATE INDEX takes only a
-- SHARE lock (reads continue, writes wait), and the ACCESS EXCLUSIVE lock from
-- DROP INDEX is held just until the end of the migration transaction.
CREATE INDEX jobs_dead_new ON jobs (queue, finished_at, id) WHERE state = 'dead';
DROP INDEX jobs_dead;
ALTER INDEX jobs_dead_new RENAME TO jobs_dead;
