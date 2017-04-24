# Task list

• Proof	Team-wise	(must	have)
- [x] Cluster	replication	or	sharding as	per	individual	team	goals
- [x] RAFT	– working	(cold	start,	node	addition,	node	removal,	leader	lost)
- [ ] Work	Stealing	– What	is	stolen?	Proof	that	tasks	are	stolen
- [x] Support	for	class	determined	functional	behavior
- [x] A java	client	is	okay
- [ ] Not	using	IDE	(e.g.,	Eclipse)	to	run	your	servers

• Global	(a.k.a.	Class)	Proof	(gosh-my-grade-would-love-to-have-this)
- [x] Ping	round	trip	– ground	truth	that	the	global	communication	is	working
- [ ] Write	injection	– Single	file,	Multiple	files,	really-really-big-files
- [ ] Read	a	local	file	(read	your	own	writes)
- [ ] Read	remote	file	(not	on	your	cluster)
- [ ] Any	client	can	connect	to	any	cluster
- [ ] Unknown	requirements	and	expectations

# Current behavior
1. Continuing with storing chunks in-memory as not listed for testing in team. Hence file sizes should be limited to less than a GB.
2. Each node has a redis node which is updated with the leader value whenever a new leader is elected. This value can be read by any client  to connect to a leader.
3. Redis will store metadata of all files
4. MySQl should be able to store chunks 

# Tomorrow Goals
4. Redis will store chunks of last 3 files (hot storage)

