import models  # noqa
from constant import UNIT_10K
from database import session
from utils import show_run_time


class TestFactories:

    #
    #     @show_run_time
    #     def test_run_int_1m(self):
    #         for i in range(100 * UNIT_10K):
    #             next(fuzzyInt(6))
    #
    #     @show_run_time
    #     def test_run_int_10m(self):
    #         for i in range(1000 * UNIT_10K):
    #             next(fuzzyInt(6))
    #
    @show_run_time
    def test_run_uuid_1m(self):
        for i in range(100 * UNIT_10K):
            next(fuzzyUUID())

    @show_run_time
    def test_run_uuid_10m(self):
        for i in range(1000 * UNIT_10K):
            next(fuzzyUUID())

    @show_run_time
    def test_run_uuidstr_1m(self):
        for i in range(100 * UNIT_10K):
            next(fuzzyUUIDStr())

    @show_run_time
    def test_run_uuidstr_10m(self):
        for i in range(1000 * UNIT_10K):
            next(fuzzyUUIDStr())

    # @show_run_time
    # def test_run_uuidstr_v2_10m(self):
    #     for i in range(1000 * UNIT_10K):
    #         next(fuzzyUUIDStrV2())

    @show_run_time
    def test_run_str_1m(self):
        for i in range(100 * UNIT_10K):
            next(fuzzyString())

    @show_run_time
    def test_run_str_10m(self):
        for i in range(1000 * UNIT_10K):
            next(fuzzyString())

    @show_run_time
    def test_run_email_1m(self):
        for i in range(100 * UNIT_10K):
            next(fuzzyEmail())

    @show_run_time
    def test_run_email_10m(self):
        for i in range(1000 * UNIT_10K):
            next(fuzzyEmail())


@show_run_time
def run():
    data = []
    sess = session()

    for _ in range(100):
        cpx = Complex().one()
        complex_uuid = cpx.uuid

        data.append(cpx)

        sess.add(cpx)

        for _ in range(10):
            screen = Screen().one(complex_uuid=complex_uuid)
            screen_uuid = screen.uuid
            device = Device().one(complex_uuid=complex_uuid, screen_uuid=screen_uuid)

            data.extend([screen, device])

            sess.add(screen)
            sess.add(device)

    sess.commit()
    return data
