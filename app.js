const Koa = require('koa');
const Router = require('koa-router');
const bodyParser = require('koa-bodyparser');
const knex = require('knex');
const Joi = require('@hapi/joi').extend(require('@hapi/joi-date'));

const app = new Koa();
app.use(bodyParser());
app.context.db = knex({
    client: 'pg',
    connection: {
        host : '127.0.0.1',
        user : 'postgres',
        password : 'postgres',
        database : 'moyklass'
    }
});

app.use(async (ctx, next) => {
    if (!ctx.is('application/json')) {
        ctx.throw(500);
    }
    await next();
});

const router = new Router();

const rootSchema = Joi.object({
    date: Joi.any().custom((value, helpers) => {
        const validate = Joi.array().items(Joi.date().format('YYYY-MM-DD').utc()).max(2).validate(String(value).split(','));
        if (validate.error) {
            throw new Error(validate.error.message);
        } else {
            return validate.value;
        }
    }, 'custom split date').default(null),
    status: Joi.number().integer().min(0).max(1).default(null),
    teacherIds: Joi.any().custom((value, helpers) => {
        const validate = Joi.array().items(Joi.number().integer()).validate(String(value).split(',').map(v => Number(v)));
        if (validate.error) {
            throw new Error(validate.error.message);
        } else {
            return validate.value;
        }
    }, 'custom split number').default(null),
    studentsCount: Joi.any().custom((value, helpers) => {
        const validate = Joi.array().items(Joi.number().integer()).max(2).validate(String(value).split(',').map(v => Number(v)));
        if (validate.error) {
            throw new Error(validate.error.message);
        } else {
            return validate.value;
        }
    }, 'custom split number').default(null),
    page: Joi.number().integer().default(1),
    lessonsPerPage: Joi.number().integer().default(5),
});

router.all('/', async (ctx, next) => {

    const validate = rootSchema.validate(ctx.request.body);

    if (validate.error) {
        console.error(validate.error.message);
        ctx.throw(400, validate.error.message);
    }

    const params = validate.value;

    const sql = `
        SELECT
          les.id,
          les.date::TEXT,
          les.title,
          les.status,
          stus."visitCount",
          stus."students",
          teas."teachers"
        FROM
          public.lessons les,
          LATERAL (
            SELECT
              count(1) "studentsCount",
              sum(les_stu.visit::INT) "visitCount",
              jsonb_agg(jsonb_build_object('id', stu.id, 'name', stu.name, 'visit', les_stu.visit)) "students"
            FROM
              public.lesson_students les_stu
              INNER JOIN public.students stu ON (stu.id = les_stu.student_id)
            WHERE
               les_stu.lesson_id = les.id
          ) stus,
          LATERAL (
            SELECT
              jsonb_agg(jsonb_build_object('id', tea.id, 'name', tea.name)) "teachers"
            FROM
              public.lesson_teachers les_tea
              INNER JOIN public.teachers tea ON (tea.id = les_tea.teacher_id)
            WHERE
               les_tea.lesson_id = les.id
               AND
               (:teacherIds::INT[] IS NULL OR les_tea.teacher_id = ANY(:teacherIds::INT[]))
          ) teas
        WHERE
          (:date::DATE[] IS NULL OR les.date BETWEEN (:date::DATE[])[1] AND COALESCE((:date::DATE[])[2], (:date::DATE[])[1]))
          AND
          (:status::INT IS NULL OR les.status = :status::INT)
          AND
          (:studentsCount::INT[] IS NULL OR stus."studentsCount" BETWEEN (:studentsCount::INT[])[1] AND COALESCE((:studentsCount::INT[])[2], (:studentsCount::INT[])[1]))
          AND
          (NOT :teacherIds::INT[] IS NULL AND NOT teas IS NULL)
        ORDER BY
          les.date
        OFFSET (:lessonsPerPage::INT * (:page::INT - 1))
        LIMIT :lessonsPerPage::INT
    `;

    const [rows, err] = await ctx.db.raw(sql, params).then(result => [result.rows, null]).catch(err => [null, err]);

    if (err) {
        ctx.throw(400, err.toString());
    } else {
        ctx.body = rows;
    }

});

const lessonsSchema = Joi.object({
    teacherIds: Joi.array().items(Joi.number().integer()).default(null),
    title: Joi.string().max(100).required(),
    days: Joi.array().items(Joi.number().integer().min(0).max(6)).min(1).max(7).required(),
    firstDate: Joi.date().format('YYYY-MM-DD').utc().required(),
    lessonsCount: Joi.number().integer(),
    lastDate: Joi.date().format('YYYY-MM-DD').utc(),
}).xor('lessonsCount', 'lastDate');

router.all('/lessons', async (ctx, next) => {

    const validate = lessonsSchema.validate(ctx.request.body);

    if (validate.error) {
        console.error(validate.error.message);
        ctx.throw(400, validate.error.message);
    }

    const params = {
        ...{
            lessonsCount: null,
            lastDate: null,
            lessonsMax: 300,
            intervalMax: '1 year',
        },
        ...validate.value,
    };

    const sql = `
        WITH lessons_new AS (
          INSERT INTO public.lessons (date, title)
          SELECT
            "dates".date,
            :title::TEXT "title"
          FROM
            (
              SELECT
                (row_number() over()) "rowNumber",
                "date"
              FROM
                generate_series(
                  :firstDate::DATE,
                  CASE 
                    WHEN NOT :lessonsCount::INT IS NULL THEN :firstDate::DATE + (ceil(:lessonsCount::numeric / array_length(:days::INT[], 1)::numeric) * interval '1 weeks')
                    WHEN NOT :lastDate::DATE IS NULL THEN :lastDate::DATE
                    ELSE :firstDate::DATE
                  END,
                  '1 day'
                ) "date"
              WHERE
                EXTRACT(DOW FROM "date") = ANY (:days::INT[])
            ) "dates"
          WHERE
            (dates."rowNumber" <= COALESCE(:lessonsCount::INT, :lessonsMax::INT))
            AND
            (dates.date <= COALESCE(:lastDate::DATE, :firstDate::DATE + :intervalMax::INTERVAL))
          RETURNING *
        ), lesson_teachers AS (
          INSERT INTO public.lesson_teachers (lesson_id, teacher_id)
          SELECT
            les.id "lesson_id",
            "teacher_id"
          FROM
            lessons_new "les",
            unnest(:teacherIds::INT[]) "teacher_id"
          ORDER BY
            les.id,
            "teacher_id"  
          RETURNING *
        )
        SELECT "les".id FROM lessons_new "les"
    `;

    const [rows, err] = await ctx.db.raw(sql, params).then(result => [result.rows, null]).catch(err => [null, err]);

    if (err) {
        ctx.throw(400, err.toString());
    } else {
        ctx.body = rows.map(row => row.id);
    }

});

app.use(router.routes());
app.use(router.allowedMethods());

app.listen(3000);
