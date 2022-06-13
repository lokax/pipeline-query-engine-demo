# pipeline-query-engine-demo
```c++

/*                               Limit         |<without push> <early break>
 *                                 |           |
 * (push the data to ht)        HashJoin       |
 * |<build ht>                 |         |     |<probe ht>
 * |                         Filter     Scan   |
 * |                           |               (piepeline 2)
 * |                          Scan             (get data from source operator)
 * (pipeline 1)
 *
 *
 */

/*
 * 1、Get Data From Source
 * 2、Apply Op To Data
 * 3、<Optional> Push Data To ...
 *
 * Pipleline 2 should depend on pipeline 1
 *
 *
 * Advantage:
 * 1、Data flow control is extracted to the pipeline executor(The phy-executor just keep computational logic)
 * 2、Ability to schedule pipelines tasks in parallel(TODO:)
*/
```
